SELECT dept, SUM(weekly_sales)
FROM {{ ref('mart_sales_store_date_dept') }}
GROUP BY dept