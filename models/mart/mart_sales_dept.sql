SELECT 
    dept, 
    Total_Weekly_Sales,
    RANK() OVER (ORDER BY Total_Weekly_Sales DESC) AS Total_Sales_Rank,
    Average_Weekly_Sales,
    RANK() OVER (ORDER BY Average_Weekly_Sales DESC) AS Average_Weekly_Sales_Rank,
    Weekly_Sales_Size_Sqm,
    RANK() OVER (ORDER BY Weekly_Sales_Size_Sqm DESC) AS Weekly_Sales_Size_Sqm_Rank,
    Weigted_Weekly_Sales,
    RANK() OVER (ORDER BY Weigted_Weekly_Sales DESC) AS Weigted_Weekly_Sales_Rank,
    Weigted_Weekly_Sales_Sales_Size_Sqm,
    RANK() OVER (ORDER BY Weigted_Weekly_Sales_Sales_Size_Sqm DESC) AS Weigted_Weekly_Sales_Sales_Size_Sqm_Rank, 
    Average_Temperature_Celsius,
    Average_Fuel_Price,
    Markdown1, 
    Markdown2, 
    Markdown3, 
    Markdown4, 
    Markdown5,
    Sum_Total_Markdown,
    ROUND((Sum_Total_Markdown / Total_Weekly_Sales),2) AS Promo_Rate,
    ROUND(Total_Weekly_Sales - Sum_Total_Markdown, 2) AS Sum_Net_Total_Sales,
    ROUND((Total_Weekly_Sales - Sum_Total_Markdown) / NULLIF(Total_Weekly_Sales, 0), 2) AS Markdown_Rate,
    Average_CPI,
    Average_Unemployment

FROM (
SELECT 
    dept, 
    ROUND(SUM(Weekly_Sales),2) AS Total_Weekly_Sales, 
    ROUND(AVG(Weekly_Sales),2) AS Average_Weekly_Sales, 
    ROUND(SUM(Weekly_Sales_Size_Sqm),2) AS Weekly_Sales_Size_Sqm, 
    ROUND(SUM(Weigted_Weekly_Sales_Sales_Size_Sqm), 2) AS Weigted_Weekly_Sales_Sales_Size_Sqm,
    ROUND(SUM(Weigted_Weekly_Sales), 2) AS Weigted_Weekly_Sales,
    ROUND(AVG(Temperature_Celsius),2) AS Average_Temperature_Celsius,  
    ROUND(AVG(Fuel_Price),2) AS Average_Fuel_Price,
    ROUND(SUM(Markdown1),2) AS Markdown1, 
    ROUND(SUM(Markdown2),2) AS Markdown2, 
    ROUND(SUM(Markdown3),2) AS Markdown3, 
    ROUND(SUM(Markdown4),2) AS Markdown4, 
    ROUND(SUM(Markdown5),2) AS Markdown5,
    ROUND(SUM(Total_Markdown),2) AS Sum_Total_Markdown,
    ROUND(AVG(CPI),2) AS Average_CPI,
    ROUND(AVG(Unemployment),2) AS Average_Unemployment

FROM {{ ref('mart_sales_store_date_dept') }}
GROUP BY dept
) AS subquery
ORDER BY dept ASC