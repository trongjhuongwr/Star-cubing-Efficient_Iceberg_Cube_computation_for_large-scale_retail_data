TRUNCATE TABLE dbo.Agg_Fact_Transactions;

INSERT INTO dbo.Agg_Fact_Transactions (
	Date_Code,
	City_Code,
	Category_Code,
	Payment_Method_Code,
	Region_Code,
	Customer_Type_Code,
	Total_Quantity,
	Total_Sales,
	Transaction_Count
)
SELECT
	f.Date_Code,
	f.City_Code,
	f.Category_Code,
	f.Payment_Method_Code,
	f.Region_Code,
	f.Customer_Type_Code,
	SUM(f.Quantity) AS Total_Quantity,
	SUM(f.Sales_Amount) AS Total_Sales,
	COUNT(*) AS Transaction_Count
FROM dbo.Fact_Transactions AS f
GROUP BY
	f.Date_Code,
	f.City_Code,
	f.Category_Code,
	f.Payment_Method_Code,
	f.Region_Code,
	f.Customer_Type_Code;
