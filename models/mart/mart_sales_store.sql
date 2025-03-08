SELECT Store, SUM(weekly_sales) AS Weekly_sales
FROM {{ ref('mart_sales_store_date_dept') }}
GROUP BY Store
ORDER BY Store