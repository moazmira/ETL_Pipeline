CREATE TABLE dimgeography (
    GeographyKey    INT             NOT NULL IDENTITY(1,1),
    City            NVARCHAR(100)    NULL,
    State           NVARCHAR(100)    NULL,
    CountryRegion   NVARCHAR(100)    NULL,
    Continent       NVARCHAR(100)    NULL,

    CONSTRAINT PK_dim_geography PRIMARY KEY (GeographyKey)
);
