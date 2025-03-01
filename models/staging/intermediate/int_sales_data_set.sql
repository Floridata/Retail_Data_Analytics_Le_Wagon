SELECT 
    Store, 
    Dept, 
    Date AS date_date,
    Weekly_Sales, 
    IsHoliday
FROM {{ ref('stg_raw__sales') }}