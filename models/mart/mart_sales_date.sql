SELECT
    Date_date,
    IsHoliday,
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
    Temperature_Celsius,
    Temperature_Farenheit,
    Fuel_Price,
    Markdown1,
    Markdown2,
    Markdown3,
    Markdown4,
    Markdown5,
    Total_Markdown,
    Average_CPI,
    Average_Unemployment
FROM (
    SELECT
        Date_date,
        IsHoliday,
        ROUND(SUM(Weekly_Sales), 2) AS Total_Weekly_Sales,
        ROUND(AVG(Weekly_Sales), 2) AS Average_Weekly_Sales,
        ROUND(SUM(Weekly_Sales_Size_Sqf), 2) AS Weekly_Sales_Size_Sqf,
        ROUND(SUM(Weekly_Sales_Size_Sqm), 2) AS Weekly_Sales_Size_Sqm,
        ROUND(SUM(Weigted_Weekly_Sales), 2) AS Weigted_Weekly_Sales,
        ROUND(SUM(Weigted_Weekly_Sales_Sales_Size_Sqm), 2) AS Weigted_Weekly_Sales_Sales_Size_Sqm,
        ROUND(AVG(Temperature_Celsius), 2) AS Temperature_Celsius,
        ROUND(AVG(Temperature_Farenheit), 2) AS Temperature_Farenheit,
        ROUND(AVG(Fuel_Price), 2) AS Fuel_Price,
        ROUND(SUM(Markdown1), 2) AS Markdown1,
        ROUND(SUM(Markdown2), 2) AS Markdown2,
        ROUND(SUM(Markdown3), 2) AS Markdown3,
        ROUND(SUM(Markdown4), 2) AS Markdown4,
        ROUND(SUM(Markdown5), 2) AS Markdown5,
        ROUND(SUM(Total_Markdown), 2) AS Total_Markdown,
        ROUND(AVG(CPI), 2) AS Average_CPI,
        ROUND(AVG(Unemployment), 2) AS Average_Unemployment
    FROM {{ ref('mart_sales_store_date_dept') }}
    GROUP BY Date_date, IsHoliday
) AS subquery
ORDER BY Date_date ASC
