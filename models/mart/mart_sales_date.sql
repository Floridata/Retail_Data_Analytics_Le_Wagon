SELECT Date_date, SUM(weekly_sales) AS Weekly_sales
FROM {{ ref('mart_sales_store_date_dept') }} 
GROUP BY Date_date
ORDER BY Date_date