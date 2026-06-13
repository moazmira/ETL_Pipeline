CREATE TABLE dimbrand (
    BrandKey        INT             NOT NULL,
    BrandName       NVARCHAR(100)   NOT NULL,

    CONSTRAINT PK_dim_brand PRIMARY KEY (BrandKey)
);

CREATE TABLE dimgeography (
    GeographyKey    INT             NOT NULL IDENTITY(1,1),
    City            NVARCHAR(100)    NULL,
    State           NVARCHAR(100)    NULL,
    CountryRegion   NVARCHAR(100)    NULL,
    Continent       NVARCHAR(100)    NULL,

    CONSTRAINT PK_dim_geography PRIMARY KEY (GeographyKey)
);


CREATE TABLE dimdate (
    DateKey         DATE            NOT NULL,
    Year            INT             NOT NULL,
    Quarter         INT             NOT NULL,      
    QuarterName     NVARCHAR(2)     NOT NULL,      
    YearQuarter     NVARCHAR(7)     NOT NULL,     
    MonthNumber     INT             NOT NULL,     
    MonthName       NVARCHAR(10)    NOT NULL,     
    MonthShort      NVARCHAR(3)     NOT NULL,     
    YearMonth       NVARCHAR(7)     NOT NULL,      
    Day             INT             NOT NULL,      
    DayOfWeekNum    INT             NOT NULL,      
    DayName         NVARCHAR(10)    NOT NULL,     
    WeekOfYear      INT             NOT NULL,     
    IsWeekend       BIT             NOT NULL,      

    CONSTRAINT PK_dim_date PRIMARY KEY (DateKey)
);


CREATE TABLE dimproduct (
    ProductKey      INT             NOT NULL,
    ProductName     NVARCHAR(255)    NULL,
    BrandKey        INT             NOT NULL,      
    Subcategory     NVARCHAR(100)    NULL,
    Category        NVARCHAR(100)    NULL,

    CONSTRAINT PK_dim_product   PRIMARY KEY (ProductKey),
    CONSTRAINT FK_product_brand FOREIGN KEY (BrandKey)
        REFERENCES dimbrand (BrandKey)
);


CREATE TABLE dim_customer (
    CustomerKey     INT             NOT NULL,
    CustomerCode    NVARCHAR(20)    NOT NULL,
    FirstName       NVARCHAR(100)    NULL  DEFAULT 'Unknown',
    LastName        NVARCHAR(100)    NULL  DEFAULT 'Unknown',
    Education       NVARCHAR(100)   NOT NULL  DEFAULT 'Unknown',
    Occupation      NVARCHAR(100)   NOT NULL  DEFAULT 'Unknown',
    GeographyKey    INT             NOT NULL,      

    CONSTRAINT PK_dim_customer      PRIMARY KEY (CustomerKey),
    CONSTRAINT FK_customer_geo      FOREIGN KEY (GeographyKey)
        REFERENCES dimgeography (GeographyKey)
);


CREATE TABLE fact_sales (
    DateKey         DATE            NOT NULL,      
    ProductKey      INT             NOT NULL,      
    CustomerKey     INT             NOT NULL,      
    Quantity        INT             NOT NULL,
    NetPrice        DECIMAL(18,4)   NOT NULL,
    Revenue         DECIMAL(18,4)   NOT NULL,      

    CONSTRAINT FK_sales_date        FOREIGN KEY (DateKey)
        REFERENCES dimdate (DateKey),
    CONSTRAINT FK_sales_product     FOREIGN KEY (ProductKey)
        REFERENCES dimproduct (ProductKey),
    CONSTRAINT FK_sales_customer    FOREIGN KEY (CustomerKey)
        REFERENCES dim_customer (CustomerKey)
);


CREATE TABLE fact_forecast (
    ForecastID      INT             NOT NULL IDENTITY(1,1),
    Year            INT             NOT NULL,      
    BrandKey        INT             NOT NULL,      
    GeographyKey    INT             NOT NULL,      
    Forecast        INT             NOT NULL,

    CONSTRAINT PK_fact_forecast     PRIMARY KEY (ForecastID),
    CONSTRAINT FK_forecast_brand    FOREIGN KEY (BrandKey)
        REFERENCES dimbrand (BrandKey),
    CONSTRAINT FK_forecast_geo      FOREIGN KEY (GeographyKey)
        REFERENCES dimgeography (GeographyKey)
);


