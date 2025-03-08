SELECT dept, SUM(weekly_sales) AS Weekly_sales
FROM {{ ref('mart_sales_store_date_dept') }}
GROUP BY dept
ORDER BY dept