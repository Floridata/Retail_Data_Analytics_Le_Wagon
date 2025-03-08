SELECT 
    s.Dept,
    ROUND(SUM(s.Weekly_Sales), 2) AS Total_Sales,
    ROUND(AVG(s.Weekly_Sales), 2) AS Avg_Sales,
    ROUND(SUM(IFNULL(f.MarkDown1, 0)), 2) AS Total_MarkDown1,
    ROUND(SUM(IFNULL(f.MarkDown2, 0)), 2) AS Total_MarkDown2,
    ROUND(SUM(IFNULL(f.MarkDown3, 0)), 2) AS Total_MarkDown3,
    ROUND(SUM(IFNULL(f.MarkDown4, 0)), 2) AS Total_MarkDown4,
    ROUND(SUM(IFNULL(f.MarkDown5, 0)), 2) AS Total_MarkDown5,
    ROUND(
        SUM(IFNULL(f.MarkDown1, 0)) + SUM(IFNULL(f.MarkDown2, 0)) + 
        SUM(IFNULL(f.MarkDown3, 0)) + SUM(IFNULL(f.MarkDown4, 0)) + 
        SUM(IFNULL(f.MarkDown5, 0)), 
    2) AS Total_MarkDown,
    ROUND(
        SUM(s.Weekly_Sales) - (
            SUM(IFNULL(f.MarkDown1, 0)) + SUM(IFNULL(f.MarkDown2, 0)) + 
            SUM(IFNULL(f.MarkDown3, 0)) + SUM(IFNULL(f.MarkDown4, 0)) + 
            SUM(IFNULL(f.MarkDown5, 0))
        ), 
    2) AS Margin_After_MarkDown,
    ROUND(AVG(f.Temperature_Celsius), 2) AS Avg_Temperature,
    ROUND(AVG(f.Fuel_Price), 2) AS Avg_Fuel_Price,
    ROUND(AVG(IFNULL(f.CPI, 0)), 2) AS Avg_CPI,
    ROUND(AVG(IFNULL(f.Unemployment, 0)), 2) AS Avg_Unemployment
FROM {{ ref('int_sales_data_set') }} AS s
LEFT JOIN {{ ref('int_features_data_set') }} AS f ON s.Store = f.Store
LEFT JOIN {{ ref('int_stores_data_set') }} AS st ON s.Store = st.Store
GROUP BY s.Dept
ORDER BY Dept ASC