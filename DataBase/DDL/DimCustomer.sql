CREATE TABLE DimCustomer (
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