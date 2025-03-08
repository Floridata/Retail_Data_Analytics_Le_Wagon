SELECT 
    Store_dept_date,
    S.Store,
    ST.Type,
    ROUND(Size_sqf, 2) AS Size_sqf, 
    ROUND(Size_sqm, 2) AS Size_sqm, 
    S.Date_date, 
    Dept,  
    ROUND(Weekly_Sales, 2) AS Weekly_Sales, 
    ROUND(Weekly_Sales / Size_sqf, 2) AS Weekly_Sales_Size_Sqf, 
    ROUND(Weekly_Sales / Size_sqm, 2) AS Weekly_Sales_Size_Sqm, 
    ROUND(Weekly_Sales * (CPI / 171.2 * 100), 2) AS Weigted_Weekly_Sales,
    ROUND(Weekly_Sales * (CPI / 171.2 * 100) / Size_sqf,2) AS Weigted_Weekly_Sales_Size_Sqf,
    ROUND(Weekly_Sales * (CPI / 171.2 * 100) / Size_sqm,2) AS Weigted_Weekly_Sales_Sales_Size_Sqm,
    ROUND(Temperature_Celsius, 2) AS Temperature_Celsius, 
    ROUND(Temperature_Farenheit, 2) AS Temperature_Farenheit, 
    ROUND(Fuel_Price, 2) AS Fuel_Price,  
    ROUND(Markdown1, 2) AS Markdown1, 
    ROUND(Markdown2, 2) AS Markdown2, 
    ROUND(Markdown3, 2) AS Markdown3, 
    ROUND(Markdown4, 2) AS Markdown4, 
    ROUND(Markdown5, 2) AS Markdown5, 
    ROUND(Markdown1 + Markdown2 + Markdown3 + Markdown4 + Markdown5, 2) AS Total_Markdown, 
    ROUND(CPI, 2) AS CPI, 
    ROUND(Unemployment, 2) AS Unemployment,
    IsHoliday
FROM 
    {{ ref('int_sales_data_set') }} S
LEFT JOIN 
    {{ ref('int_features_data_set') }} F
    ON S.Store = F.Store 
    AND S.Date_date = F.Date_date
LEFT JOIN 
    {{ ref('int_stores_data_set') }} ST
    ON S.Store = ST.Store

