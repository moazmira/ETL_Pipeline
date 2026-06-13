# ============================================================
#  ETL Pipeline — Sales & Forecast JSON → SQL Server
#  Author  : Data Engineering Team
#  Version : 1.0.0
#  Schema  : Galaxy Schema (2 Facts, 4 Dims, 1 Date)
# ============================================================

import os
import sys
import json
import logging
import traceback
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError


# ============================================================
#  0. BOOTSTRAP — Load .env + configure logging
# ============================================================

load_dotenv()



LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

LOG_FILE = f"logs/etl_{timestamp}.log"

Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger("ETL")

logger.info(f"Log File Created: {LOG_FILE}")


def build_engine():
 
    server   = os.getenv("DB_SERVER")
    port     = os.getenv("DB_PORT", "1433")
    database = os.getenv("DB_NAME")
    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([server, database, user, password]):
        raise EnvironmentError(
            "Missing one or more DB credentials in .env: "
            "DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD"
        )

    drivers = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]

    for driver in drivers:
        try:
            conn_str = (
                f"mssql+pyodbc://{user}:{password}@{server},{port}/{database}"
                f"?driver={driver.replace(' ', '+')}"
                f"&TrustServerCertificate=yes"
            )
            engine = create_engine(
                conn_str,
                fast_executemany=True,   
                pool_pre_ping=True,      
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Connected to SQL Server using driver: [{driver}]")
            return engine
        except Exception:
            logger.debug(f"Driver [{driver}] failed, trying next...")

    raise ConnectionError(
        "Could not connect to SQL Server with any available ODBC driver. "
        "Install 'ODBC Driver 17 for SQL Server' or later."
    )



def extract_sales(path: str) -> pd.DataFrame:
    """Load Sales.json into a raw DataFrame."""
    logger.info(f"Extracting Sales data from: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        logger.info(f"Sales raw rows loaded: {len(df):,}")
        return data,df
    except FileNotFoundError:
        raise FileNotFoundError(f"Sales JSON not found at path: {path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Sales JSON is malformed: {e}")





def extract_forecast(path: str) -> pd.DataFrame:
    """Load forecast.json into a raw DataFrame."""
    logger.info(f"Extracting Forecast data from: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        logger.info(f"Forecast raw rows loaded: {len(df):,}")
        return data, df
    except FileNotFoundError:
        raise FileNotFoundError(f"Forecast JSON not found at path: {path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Forecast JSON is malformed: {e}")


def data_exploration_report(
    raw_json: list[dict],
    df: pd.DataFrame,
    dataset_name: str,
    date_columns: list[str] | None = None,
):
    logger.info("=" * 60)
    logger.info(f"DATA EXPLORATION REPORT [{dataset_name}]")
    logger.info("=" * 60)

    # [1] STRUCTURE

    logger.info("[1] STRUCTURE")
    logger.info(f"Rows    : {len(df):,}")
    logger.info(f"Columns : {len(df.columns):,}")

    # [2] TYPE PROFILING

    logger.info("[2] TYPE PROFILING")

    for col in df.columns:
        logger.info(
            f"{col:<30} "
            f"{str(df[col].dtype):<12}: "
            f"{df[col].notna().sum():,}"
        )

    # [3] MISSING VALUES

    logger.info("[3] MISSING VALUES")

    missing = pd.DataFrame({
        "Missing Count": df.isna().sum(),
        "Missing %": round(df.isna().mean() * 100, 2)
    })

    missing = missing[missing["Missing Count"] > 0]

    if missing.empty:
        logger.info("No missing values found.")
    else:
        for col, row in missing.sort_values(
            "Missing Count",
            ascending=False
        ).iterrows():

            logger.warning(
                f"{col:<30} "
                f"Missing={row['Missing Count']:,.0f} "
                f"({row['Missing %']}%)"
            )

    # [4] MISSING COLUMNS PER ROW

    logger.info("[4] MISSING COLUMNS PER ROW")

    schema = sorted({
        key
        for row in raw_json
        for key in row.keys()
    })

    rows_missing_columns = 0

    for row in raw_json:

        if len(set(schema) - set(row.keys())) > 0:
            rows_missing_columns += 1

    if rows_missing_columns == 0:
        logger.info(
            "All rows contain the full expected schema."
        )
    else:
        logger.warning(
            f"Rows with missing columns: "
            f"{rows_missing_columns:,}"
        )

    # [5] DUPLICATES

    duplicates = df.duplicated().sum()

    logger.info("[5] DUPLICATE ROWS")

    if duplicates > 0:
        logger.warning(
            f"Duplicate rows: {duplicates:,} "
            f"({duplicates/len(df)*100:.2f}%)"
        )
    else:
        logger.info("No duplicate rows found.")

    # [6] UNKNOWN VALUES

    logger.info("[6] UNKNOWN VALUES")

    for col in df.select_dtypes(include="object"):

        count = (
            df[col]
            .astype(str)
            .str.strip()
            .isin([
                "",
                "Unknown",
                "None",
                "nan",
                "NaN"
            ])
            .sum()
        )

        if count:
            logger.warning(
                f"{col:<30} Unknown Values: {count:,}"
            )

    # [7] DATE VALIDATION

    if date_columns:

        logger.info("[7] DATE VALIDATION")

        for col in date_columns:

            parsed = pd.to_datetime(
                df[col],
                errors="coerce"
            )

            invalid = parsed.isna().sum()

            if invalid:
                logger.warning(
                    f"{col}: {invalid:,} invalid dates"
                )
            else:
                logger.info(
                    f"{col}: all dates valid"
                )

    # [8] COLUMN WHITESPACE

    logger.info("[8] WHITESPACE ISSUES")

    bad_cols = [
        c for c in df.columns
        if c != c.strip()
    ]

    if bad_cols:

        for col in bad_cols:
            logger.warning(
                f"Column contains leading/trailing spaces: '{col}'"
            )
    else:
        logger.info(
            "No whitespace issues found."
        )

    logger.info("=" * 60)

#  3. TRANSFORM — Cleaning helpers

UNKNOWN = "Unknown"


def _clean_string(series: pd.Series) -> pd.Series:
    """Strip whitespace; replace empty/null strings with 'Unknown'."""
    return (
        series.astype(str)
              .str.strip()
              .replace({"": UNKNOWN, "None": UNKNOWN, "nan": UNKNOWN, "NaN": UNKNOWN})
    )


def _fill_nulls_string(series: pd.Series) -> pd.Series:
    return series.fillna(UNKNOWN).astype(str).str.strip()


def _parse_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, format="%m/%d/%Y").dt.date


def _split_name(name_series: pd.Series):
    def split(val):
        if pd.isna(val) or str(val).strip() in ("", "None", "nan"):
            return UNKNOWN, UNKNOWN
        parts = str(val).split(",", 1)
        last  = parts[0].strip() if parts[0].strip() else UNKNOWN
        first = parts[1].strip() if len(parts) > 1 and parts[1].strip() else UNKNOWN
        return last, first

    split_result = name_series.apply(split)
    last_names  = split_result.apply(lambda x: x[0])
    first_names = split_result.apply(lambda x: x[1])
    return last_names, first_names


#  a. TRANSFORM — Dimension tables

def transform_dim_brand(df_sales: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming DimBrand...")
    brands = (
        df_sales["Brand"]
        .dropna()
        .str.strip()
        .unique()
    )
    dim = pd.DataFrame({
        "BrandName": sorted(brands),
    })
    dim.insert(0, "BrandKey", range(1, len(dim) + 1))
    logger.info(f"DimBrand rows: {len(dim)}")
    return dim


def transform_dim_geography(df_sales: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming DimGeography...")

    city_level = (
        df_sales[["City", "State", "CountryRegion", "Continent"]]
        .drop_duplicates()
        .copy()
    )
    for col in ["City", "State", "CountryRegion", "Continent"]:
        city_level[col] = _clean_string(city_level[col])

    country_level = (
        city_level[["CountryRegion", "Continent"]]
        .drop_duplicates()
        .copy()
    )
    country_level["City"]  = UNKNOWN
    country_level["State"] = UNKNOWN

    dim = (
        pd.concat([city_level, country_level], ignore_index=True)
          .drop_duplicates()
          .reset_index(drop=True)
    )
    dim.insert(0, "GeographyKey", range(1, len(dim) + 1))
    logger.info(f"DimGeography rows: {len(dim)} (city-level + country-level)")
    return dim


def transform_dim_product(df_sales: pd.DataFrame, dim_brand: pd.DataFrame) -> pd.DataFrame:

    logger.info("Transforming DimProduct...")

    dim = (
        df_sales[["ProductKey", "Product Name", "Brand", "Subcategory", "Category"]]
        .drop_duplicates(subset=["ProductKey"])
        .copy()
        .rename(columns={"Product Name": "ProductName"})
    )
    for col in ["ProductName", "Brand", "Subcategory", "Category"]:
        dim[col] = _clean_string(dim[col])

    brand_map = dim_brand.set_index("BrandName")["BrandKey"]
    dim["BrandKey"] = dim["Brand"].map(brand_map)
    dim = dim[["ProductKey", "ProductName", "BrandKey", "Subcategory", "Category"]]
    logger.info(f"DimProduct rows: {len(dim)}")
    return dim


def transform_dim_customer(df_sales: pd.DataFrame, dim_geography: pd.DataFrame) -> pd.DataFrame:
 
    logger.info("Transforming DimCustomer...")

    dim = (
        df_sales[[
            "CustomerKey", "Customer Code", "Name",
            "Education", "Occupation",
            "City", "State", "CountryRegion"
        ]]
        .drop_duplicates(subset=["CustomerKey"])
        .copy()
        .rename(columns={"Customer Code": "CustomerCode"})
    )

    # Split Name
    dim["LastName"], dim["FirstName"] = _split_name(dim["Name"])

    # Fill nulls
    dim["Education"]  = _fill_nulls_string(dim["Education"])
    dim["Occupation"] = _fill_nulls_string(dim["Occupation"])

    # Clean geo cols for lookup
    for col in ["City", "State", "CountryRegion"]:
        dim[col] = _clean_string(dim[col])

    # Lookup GeographyKey (city-level rows)
    geo_map = (
        dim_geography[dim_geography["City"] != UNKNOWN]
        .set_index(["City", "State", "CountryRegion"])["GeographyKey"]
    )
    dim["GeographyKey"] = (
        dim.set_index(["City", "State", "CountryRegion"])
           .index.map(geo_map)
           .values
    )
    dim["GeographyKey"] = dim["GeographyKey"].fillna(-1).astype(int)

    dim = dim[[
        "CustomerKey", "CustomerCode",
        "FirstName", "LastName",
        "Education", "Occupation",
        "GeographyKey"
    ]]
    logger.info(f"DimCustomer rows: {len(dim)}")
    return dim


def transform_dim_date(start: date = date(2008, 1, 1),
                       end:   date = date(2009, 12, 31)) -> pd.DataFrame:
 
    logger.info(f"Building DimDate from {start} to {end}...")

    days = pd.date_range(start=start, end=end, freq="D")
    dim  = pd.DataFrame({"DateKey": days.date})
    dim["Year"]         = days.year
    dim["Quarter"]      = days.quarter
    dim["QuarterName"]  = "Q" + days.quarter.astype(str)
    dim["YearQuarter"]  = days.year.astype(str) + "-Q" + days.quarter.astype(str)
    dim["MonthNumber"]  = days.month
    dim["MonthName"]    = days.strftime("%B")
    dim["MonthShort"]   = days.strftime("%b")
    dim["YearMonth"]    = days.strftime("%Y-%m")
    dim["Day"]          = days.day
    dim["DayOfWeekNum"] = days.dayofweek + 1          # 1=Mon … 7=Sun
    dim["DayName"]      = days.strftime("%A")
    dim["WeekOfYear"]   = days.isocalendar().week.values
    dim["IsWeekend"]    = days.dayofweek.isin([5, 6]).astype(int)

    logger.info(f"DimDate rows: {len(dim)}")
    return dim



def transform_fact_sales(df_sales: pd.DataFrame) -> pd.DataFrame:
 
    logger.info("Transforming FactSales...")

    before = len(df_sales)
    df = df_sales.drop_duplicates().copy()
    logger.info(f"Duplicates removed: {before - len(df):,} | Remaining: {len(df):,}")

    df["DateKey"]  = _parse_dates(df["OrderDate"])
    df["NetPrice"] = pd.to_numeric(df["Net Price"], errors="coerce").round(4)
    df["Quantity"] = pd.to_numeric(df["Quantity"],  errors="coerce").fillna(0).astype(int)
    df["Revenue"]  = (df["Quantity"] * df["NetPrice"]).round(4)

    fact = df[[
        "DateKey", "ProductKey", "CustomerKey",
        "Quantity", "NetPrice", "Revenue"
    ]].copy()

    logger.info(f"FactSales rows after transform: {len(fact):,}")
    return fact


def transform_fact_forecast(df_forecast: pd.DataFrame,
                             dim_brand: pd.DataFrame,
                             dim_geography: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming FactForecast...")

    df = df_forecast.copy()
    df["CountryRegion"] = _clean_string(df["CountryRegion"])
    df["Brand"]         = _clean_string(df["Brand"])

    brand_map = dim_brand.set_index("BrandName")["BrandKey"]
    df["BrandKey"] = df["Brand"].map(brand_map)

    # Use country-level rows (City == Unknown) for forecast geography
    geo_country = (
        dim_geography[dim_geography["City"] == UNKNOWN]
        .set_index("CountryRegion")["GeographyKey"]
    )
    df["GeographyKey"] = df["CountryRegion"].map(geo_country)

    # Validate mapping
    missing_brands = df["BrandKey"].isna().sum()
    missing_geo    = df["GeographyKey"].isna().sum()
    if missing_brands:
        logger.warning(f"FactForecast: {missing_brands} rows with unmapped BrandKey")
    if missing_geo:
        logger.warning(f"FactForecast: {missing_geo} rows with unmapped GeographyKey")

    fact = df[["Year", "BrandKey", "GeographyKey", "Forecast"]].copy()
    fact["BrandKey"]     = fact["BrandKey"].fillna(-1).astype(int)
    fact["GeographyKey"] = fact["GeographyKey"].fillna(-1).astype(int)

    logger.info(f"FactForecast rows: {len(fact)}")
    return fact


#  4. LOAD — Append-only insert with deduplication

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))


def _get_existing_keys(engine, table: str, key_cols: list[str]) -> set:

    cols_sql = ", ".join(f"[{c}]" for c in key_cols)
    query    = f"SELECT {cols_sql} FROM [{table}]"
    try:
        with engine.connect() as conn:
            existing = pd.read_sql(query, conn)
        return set(existing.itertuples(index=False, name=None))
    except Exception as e:
        logger.warning(f"Could not fetch existing keys from {table}: {e}")
        return set()


def _safe_val(v):
    import math
    if v is None:
        return None
    # numpy integers
    if hasattr(v, "item"):
        v = v.item()
    # Python float NaN → None
    if isinstance(v, float) and math.isnan(v):
        return None
    # date objects: pyodbc wants datetime or string, not datetime.date
    if type(v).__name__ == "date" and not hasattr(v, "hour"):
        from datetime import datetime
        return datetime(v.year, v.month, v.day)
    return v


def _insert_batches(engine, table: str, df: pd.DataFrame):

    total    = len(df)
    inserted = 0
    errors   = 0

    cols     = list(df.columns)
    col_sql  = ", ".join(f"[{c}]" for c in cols)
    params   = ", ".join("?" * len(cols))
    sql      = f"INSERT INTO [{table}] ({col_sql}) VALUES ({params})"

    for start in range(0, total, BATCH_SIZE):
        batch = df.iloc[start : start + BATCH_SIZE]
        rows  = [
            tuple(_safe_val(v) for v in row)
            for row in batch.itertuples(index=False, name=None)
        ]
        try:
            with engine.begin() as conn:
                # Access the raw pyodbc connection
                raw = conn.connection
                cursor = raw.cursor()
                cursor.fast_executemany = True
                cursor.executemany(sql, rows)
                cursor.close()
            inserted += len(batch)
            logger.debug(f"[{table}] Inserted batch {start}–{start + len(batch) - 1}")
        except Exception as e:
            errors += len(batch)
            logger.error(
                f"[{table}] Batch {start}–{start + len(batch) - 1} failed: {e}"
            )

    logger.info(f"[{table}] Inserted: {inserted:,} | Errors: {errors:,} | Total: {total:,}")
    return inserted, errors


def load_dimension(engine, table: str, df: pd.DataFrame, key_col: str):
    logger.info(f"Loading [{table}] — {len(df):,} transformed rows...")

    existing_keys = _get_existing_keys(engine, table, [key_col])
    if existing_keys:
        before = len(df)
        df = df[~df[key_col].isin({k[0] for k in existing_keys})]
        logger.info(f"[{table}] Skipped {before - len(df):,} existing rows, {len(df):,} new to insert")

    if df.empty:
        logger.info(f"[{table}] Nothing new to insert.")
        return 0

    return _insert_batches(engine, table, df)[0]


def load_fact_sales(engine, df: pd.DataFrame):
    table     = "FactSales"
    key_cols  = ["CustomerKey", "ProductKey", "DateKey", "Quantity", "NetPrice"]
    logger.info(f"Loading [{table}] — {len(df):,} transformed rows...")

    existing = _get_existing_keys(engine, table, key_cols)

    if existing:
        before = len(df)
        df_tuples = list(
            zip(
                df["CustomerKey"],
                df["ProductKey"],
                df["DateKey"],
                df["Quantity"],
                df["NetPrice"],
            )
        )
        mask = [t not in existing for t in df_tuples]
        df   = df[mask].reset_index(drop=True)
        logger.info(
            f"[{table}] Skipped {before - len(df):,} existing rows, "
            f"{len(df):,} new to insert"
        )

    if df.empty:
        logger.info(f"[{table}] Nothing new to insert.")
        return 0

    return _insert_batches(engine, table, df)[0]


def load_fact_forecast(engine, df: pd.DataFrame):
    table    = "FactForecast"
    key_cols = ["Year", "BrandKey", "GeographyKey"]
    logger.info(f"Loading [{table}] — {len(df):,} transformed rows...")

    existing = _get_existing_keys(engine, table, key_cols)

    if existing:
        before = len(df)
        df_tuples = list(zip(df["Year"], df["BrandKey"], df["GeographyKey"]))
        mask = [t not in existing for t in df_tuples]
        df   = df[mask].reset_index(drop=True)
        logger.info(
            f"[{table}] Skipped {before - len(df):,} existing rows, "
            f"{len(df):,} new to insert"
        )

    if df.empty:
        logger.info(f"[{table}] Nothing new to insert.")
        return 0

    return _insert_batches(engine, table, df)[0]


#  5. PIPELINE ORCHESTRATOR

def run_pipeline():
    pipeline_start = datetime.now()
    logger.info("=" * 60)
    logger.info("ETL PIPELINE STARTED")
    logger.info(f"Run timestamp: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    summary = {
        "status"    : "SUCCESS",
        "started_at": pipeline_start.isoformat(),
        "tables"    : {},
    }

    try:
        engine = build_engine()

        sales_path    = os.getenv("SALES_JSON_PATH")
        forecast_path = os.getenv("FORECAST_JSON_PATH")

        sales_raw_json, df_sales_raw = extract_sales(sales_path)

        forecast_raw_json, df_forecast_raw = extract_forecast(forecast_path)
        # DATA EXPLORATION

        data_exploration_report(
            raw_json=sales_raw_json,
            df=df_sales_raw,
            dataset_name="Sales",
            date_columns=["OrderDate"]
        )

        data_exploration_report(
            raw_json=forecast_raw_json,
            df=df_forecast_raw,
            dataset_name="Forecast"
        )

        # ── 3. Transform dimensions ────────────────────────────
        dim_brand     = transform_dim_brand(df_sales_raw)
        dim_geography = transform_dim_geography(df_sales_raw)
        dim_product   = transform_dim_product(df_sales_raw, dim_brand)
        dim_customer  = transform_dim_customer(df_sales_raw, dim_geography)
        dim_date      = transform_dim_date()

        # ── 4. Transform facts ─────────────────────────────────
        fact_sales    = transform_fact_sales(df_sales_raw)
        fact_forecast = transform_fact_forecast(df_forecast_raw, dim_brand, dim_geography)

        load_order = [
            ("DimBrand",     lambda: load_dimension(engine, "DimBrand",     dim_brand,     "BrandKey")),
            ("DimGeography", lambda: load_dimension(engine, "DimGeography", dim_geography, "GeographyKey")),
            ("DimDate",      lambda: load_dimension(engine, "DimDate",      dim_date,      "DateKey")),
            ("DimProduct",   lambda: load_dimension(engine, "DimProduct",   dim_product,   "ProductKey")),
            ("DimCustomer",  lambda: load_dimension(engine, "DimCustomer",  dim_customer,  "CustomerKey")),
            ("FactSales",    lambda: load_fact_sales(engine, fact_sales)),
            ("FactForecast", lambda: load_fact_forecast(engine, fact_forecast)),
        ]

        for table_name, load_fn in load_order:
            try:
                rows_inserted = load_fn()
                summary["tables"][table_name] = {
                    "status"        : "OK",
                    "rows_inserted" : rows_inserted,
                }
            except Exception as e:
                logger.error(f"[{table_name}] FAILED: {e}")
                logger.debug(traceback.format_exc())
                summary["tables"][table_name] = {
                    "status" : "FAILED",
                    "error"  : str(e),
                }
                summary["status"] = "PARTIAL_FAILURE"

    except Exception as e:
        logger.critical(f"Pipeline aborted: {e}")
        logger.debug(traceback.format_exc())
        summary["status"] = "FAILED"
        summary["error"]  = str(e)

    finally:
        pipeline_end = datetime.now()
        duration     = (pipeline_end - pipeline_start).total_seconds()
        summary["finished_at"]   = pipeline_end.isoformat()
        summary["duration_secs"] = round(duration, 2)

        logger.info("=" * 60)
        logger.info(f"ETL PIPELINE FINISHED — Status: {summary['status']}")
        logger.info(f"Duration: {duration:.1f}s")
        logger.info("Table Summary:")
        for tbl, info in summary.get("tables", {}).items():
            if info["status"] == "OK":
                logger.info(f"  ✓ {tbl:<20} {info['rows_inserted']:>8,} rows inserted")
            else:
                logger.error(f"  ✗ {tbl:<20} FAILED — {info.get('error','')}")
        logger.info("=" * 60)

    return summary["status"] == "SUCCESS"



if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)