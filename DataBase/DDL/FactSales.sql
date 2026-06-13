CREATE TABLE FactSales (
    DateKey         DATE            NOT NULL,      
    ProductKey      INT             NOT NULL,      
    CustomerKey     INT             NOT NULL,      
    Quantity        INT             NOT NULL,
    NetPrice        DECIMAL(18,4)   NOT NULL,
    Revenue         DECIMAL(18,4)   NOT NULL,      

    CONSTRAINT FK_sales_date        FOREIGN KEY (DateKey)
        REFERENCES DimDate (DateKey),
    CONSTRAINT FK_sales_product     FOREIGN KEY (ProductKey)
        REFERENCES DimProduct (ProductKey),
    CONSTRAINT FK_sales_customer    FOREIGN KEY (CustomerKey)
        REFERENCES DimCustomer (CustomerKey)
);
