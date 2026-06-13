CREATE TABLE DimDate (
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