CREATE TABLE DimProduct (
    ProductKey      INT             NOT NULL,
    ProductName     NVARCHAR(255)    NULL,
    BrandKey        INT             NOT NULL,      
    Subcategory     NVARCHAR(100)    NULL,
    Category        NVARCHAR(100)    NULL,

    CONSTRAINT PK_dim_product   PRIMARY KEY (ProductKey),
    CONSTRAINT FK_product_brand FOREIGN KEY (BrandKey)
        REFERENCES DimBrand (BrandKey)
);
