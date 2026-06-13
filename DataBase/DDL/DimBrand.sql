CREATE TABLE dimbrand (
    BrandKey        INT             NOT NULL,
    BrandName       NVARCHAR(100)   NOT NULL,

    CONSTRAINT PK_dim_brand PRIMARY KEY (BrandKey)
);