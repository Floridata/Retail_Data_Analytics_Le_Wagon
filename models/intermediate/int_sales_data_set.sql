SELECT 
    Store, 
    Dept, 
    Date AS Date_date,
    Weekly_Sales, 
    IsHoliday,
    CONCAT('Store ', CAST(store AS STRING),' - ', CAST(date AS STRING)) AS Store_date,
    CONCAT(CAST(Store AS STRING), '-', CAST(Dept AS STRING), '-', FORMAT_DATE('%Y%m%d', Date)) AS Store_dept_date
FROM {{ ref('stg_raw__sales') }}