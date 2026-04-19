SET NOCOUNT ON;

IF OBJECT_ID('dbo.Agg_Fact_Transactions', 'U') IS NOT NULL
	DROP TABLE dbo.Agg_Fact_Transactions;

IF OBJECT_ID('dbo.Fact_Transactions', 'U') IS NOT NULL
	DROP TABLE dbo.Fact_Transactions;

IF OBJECT_ID('dbo.Dim_Date', 'U') IS NOT NULL
	DROP TABLE dbo.Dim_Date;

IF OBJECT_ID('dbo.Dim_City', 'U') IS NOT NULL
	DROP TABLE dbo.Dim_City;

IF OBJECT_ID('dbo.Dim_Category', 'U') IS NOT NULL
	DROP TABLE dbo.Dim_Category;

IF OBJECT_ID('dbo.Dim_Payment_Method', 'U') IS NOT NULL
	DROP TABLE dbo.Dim_Payment_Method;

IF OBJECT_ID('dbo.Dim_Region', 'U') IS NOT NULL
	DROP TABLE dbo.Dim_Region;

IF OBJECT_ID('dbo.Dim_Customer_Type', 'U') IS NOT NULL
	DROP TABLE dbo.Dim_Customer_Type;

CREATE TABLE dbo.Dim_Customer_Type (
	Code INT NOT NULL PRIMARY KEY,
	Name NVARCHAR(50) NOT NULL
);

CREATE TABLE dbo.Dim_Region (
	Code INT NOT NULL PRIMARY KEY,
	Name NVARCHAR(50) NOT NULL
);

CREATE TABLE dbo.Dim_Payment_Method (
	Code INT NOT NULL PRIMARY KEY,
	Name NVARCHAR(50) NOT NULL
);

CREATE TABLE dbo.Dim_Category (
	Code INT NOT NULL PRIMARY KEY,
	Name NVARCHAR(50) NOT NULL
);

CREATE TABLE dbo.Dim_City (
	Code INT NOT NULL PRIMARY KEY,
	Name NVARCHAR(50) NOT NULL
);

CREATE TABLE dbo.Dim_Date (
	Code INT NOT NULL PRIMARY KEY,
	Name NVARCHAR(50) NOT NULL
);

CREATE TABLE dbo.Fact_Transactions (
	Date_Code INT NOT NULL,
	City_Code INT NOT NULL,
	Category_Code INT NOT NULL,
	Payment_Method_Code INT NOT NULL,
	Region_Code INT NOT NULL,
	Customer_Type_Code INT NOT NULL,
	Quantity INT NOT NULL,
	Sales_Amount FLOAT NOT NULL,
	CONSTRAINT FK_Fact_Date
		FOREIGN KEY (Date_Code) REFERENCES dbo.Dim_Date (Code),
	CONSTRAINT FK_Fact_City
		FOREIGN KEY (City_Code) REFERENCES dbo.Dim_City (Code),
	CONSTRAINT FK_Fact_Category
		FOREIGN KEY (Category_Code) REFERENCES dbo.Dim_Category (Code),
	CONSTRAINT FK_Fact_Payment_Method
		FOREIGN KEY (Payment_Method_Code) REFERENCES dbo.Dim_Payment_Method (Code),
	CONSTRAINT FK_Fact_Region
		FOREIGN KEY (Region_Code) REFERENCES dbo.Dim_Region (Code),
	CONSTRAINT FK_Fact_Customer_Type
		FOREIGN KEY (Customer_Type_Code) REFERENCES dbo.Dim_Customer_Type (Code)
);

CREATE CLUSTERED COLUMNSTORE INDEX CCI_Fact_Transactions
ON dbo.Fact_Transactions;

CREATE NONCLUSTERED INDEX IX_Fact_Date_Region_Category
ON dbo.Fact_Transactions (Date_Code, Region_Code, Category_Code)
INCLUDE (Quantity, Sales_Amount);

CREATE TABLE dbo.Agg_Fact_Transactions (
	Date_Code INT NOT NULL,
	City_Code INT NOT NULL,
	Category_Code INT NOT NULL,
	Payment_Method_Code INT NOT NULL,
	Region_Code INT NOT NULL,
	Customer_Type_Code INT NOT NULL,
	Total_Quantity INT NOT NULL,
	Total_Sales FLOAT NOT NULL,
	Transaction_Count INT NOT NULL,
	CONSTRAINT FK_Agg_Date
		FOREIGN KEY (Date_Code) REFERENCES dbo.Dim_Date (Code),
	CONSTRAINT FK_Agg_City
		FOREIGN KEY (City_Code) REFERENCES dbo.Dim_City (Code),
	CONSTRAINT FK_Agg_Category
		FOREIGN KEY (Category_Code) REFERENCES dbo.Dim_Category (Code),
	CONSTRAINT FK_Agg_Payment_Method
		FOREIGN KEY (Payment_Method_Code) REFERENCES dbo.Dim_Payment_Method (Code),
	CONSTRAINT FK_Agg_Region
		FOREIGN KEY (Region_Code) REFERENCES dbo.Dim_Region (Code),
	CONSTRAINT FK_Agg_Customer_Type
		FOREIGN KEY (Customer_Type_Code) REFERENCES dbo.Dim_Customer_Type (Code)
);

CREATE CLUSTERED INDEX IXC_Agg_Date_Region_Category
ON dbo.Agg_Fact_Transactions (Date_Code, Region_Code, Category_Code);
