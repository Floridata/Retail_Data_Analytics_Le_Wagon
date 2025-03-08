WITH aggregated_data AS (
    SELECT
        s.Store,
        SUM(s.Weekly_Sales) AS total_weekly_sales,
        AVG(s.Weekly_Sales) AS avg_weekly_sales
    FROM {{ ref('int_sales_data_set') }} AS s
    GROUP BY s.Store
),
Features_agg AS (
    SELECT
        store,
        AVG(Temperature_Celsius) AS avg_temperature_celsius,
        AVG(Temperature_Farenheit) AS avg_temperature_farenheit,
        AVG(CPI) AS avg_cpi,
        AVG(Unemployment) AS avg_unemployment,
        SUM(Markdown1) AS sum_markdown1,
        SUM(Markdown2) AS sum_markdown2,
        SUM(Markdown3) AS sum_markdown3,
        SUM(Markdown4) AS sum_markdown4,
        SUM(Markdown5) AS sum_markdown5
    FROM {{ ref('int_features_data_set') }}
    GROUP BY store
)
SELECT
    d.Store,
    d.Type,
    d.Size_sqf,
    d.Size_sqm,
    a.total_weekly_sales,
    a.avg_weekly_sales,
    f.sum_markdown1,
    f.sum_markdown2,
    f.sum_markdown3,
    f.sum_markdown4,
    f.sum_markdown5,
    (f.sum_markdown1 + f.sum_markdown2 + f.sum_markdown3 + f.sum_markdown4 + f.sum_markdown5) AS total_markdown,
    (a.total_weekly_sales - (f.sum_markdown1 + f.sum_markdown2 + f.sum_markdown3 + f.sum_markdown4 + f.sum_markdown5)) AS margin,
    f.avg_temperature_celsius,
    f.avg_temperature_farenheit,
    f.avg_cpi,
    f.avg_unemployment
FROM {{ ref('int_stores_data_set') }} AS d
LEFT JOIN aggregated_data AS a ON d.Store = a.Store
LEFT JOIN Features_agg AS f ON d.Store = f.store
