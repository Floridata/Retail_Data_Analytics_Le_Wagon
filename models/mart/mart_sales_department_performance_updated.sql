SELECT
    s.Dept AS department,
    ROUND(SUM(s.Weekly_Sales), 2) AS total_sales_per_dept,
    ROUND(AVG(s.Weekly_Sales), 2) AS Avg_Sales,
    ROUND(SUM(COALESCE(f.Markdown1, 0)), 2) AS Markdown1_per_dept,
    ROUND(SUM(COALESCE(f.Markdown2, 0)), 2) AS Markdown2_per_dept, 
    ROUND(SUM(COALESCE(f.Markdown3, 0)), 2) AS Markdown3_per_dept, 
    ROUND(SUM(COALESCE(f.Markdown4, 0)), 2) AS Markdown4_per_dept, 
    ROUND(SUM(COALESCE(f.Markdown5, 0)), 2) AS Markdown5_per_dept,
    ROUND(SUM(
        COALESCE(f.Markdown1, 0) + COALESCE(f.Markdown2, 0) + 
        COALESCE(f.Markdown3, 0) + COALESCE(f.Markdown4, 0) + 
        COALESCE(f.Markdown5, 0)), 2) AS Sum_Markdown_per_dept,
    ROUND(SUM(s.Weekly_Sales) - 
        SUM(COALESCE(f.Markdown1, 0) + COALESCE(f.Markdown2, 0) + 
        COALESCE(f.Markdown3, 0) + COALESCE(f.Markdown4, 0) + 
        COALESCE(f.Markdown5, 0)), 2) AS Margin_After_MarkDown,
    COUNT(DISTINCT s.Store) AS present_in_stores_count,
    CASE 
        WHEN COUNT(DISTINCT s.Store) = 45 THEN 'YES'
        ELSE 'NO'
    END AS Is_present_in_all_stores
FROM {{ ref('int_sales_data_set') }} AS s
LEFT JOIN {{ ref('int_features_data_set') }} AS F ON s.Store = f.Store 
LEFT JOIN {{ ref('int_stores_data_set') }} AS ST ON s.Store = st.Store
GROUP BY s.Dept
#ORDER BY Dept ASC
ORDER BY total_sales_per_dept ASC