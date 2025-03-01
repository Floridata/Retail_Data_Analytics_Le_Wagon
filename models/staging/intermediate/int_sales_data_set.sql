SELECT 
    Store, 
    Dept, 
    Date AS date_date,
    Weekly_Sales, 
    IsHoliday,
    CONCAT('Store ', CAST(store AS STRING),' - ', CAST(date AS STRING)) AS store_date
FROM {{ ref('stg_raw__sales') }}