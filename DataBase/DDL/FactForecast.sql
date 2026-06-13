CREATE TABLE factforecast (
    ForecastID      INT             NOT NULL IDENTITY(1,1),
    Year            INT             NOT NULL,      
    BrandKey        INT             NOT NULL,      
    GeographyKey    INT             NOT NULL,      
    Forecast        INT             NOT NULL,

    CONSTRAINT PK_fact_forecast     PRIMARY KEY (ForecastID),
    CONSTRAINT FK_forecast_brand    FOREIGN KEY (BrandKey)
        REFERENCES DimBrand (BrandKey),
    CONSTRAINT FK_forecast_geo      FOREIGN KEY (GeographyKey)
        REFERENCES DimGeography (GeographyKey)
);
