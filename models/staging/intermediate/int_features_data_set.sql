SELECT
  Store, 
  Date as date_date, 
  Temperature as Temperature_Farenheit,
  ROUND((Temperature-32)*5/9, 2) AS Temperature_Celsius,
  Fuel_Price,
  safe_cast(MarkDown1 AS float64) as Markdown1,
  safe_cast(MarkDown2 AS float64) as Markdown2,
  safe_cast(MarkDown3 AS float64) as Markdown3,
  safe_cast(MarkDown4 AS float64) as Markdown4,
  safe_cast(MarkDown5 AS float64) as Markdown5,
  safe_cast(CPI as float64) as CPI,
  safe_cast(Unemployment as float64) as Unemployment,
  IsHoliday,
  CONCAT('Store ', CAST(store AS STRING),' - ', CAST(date AS STRING)) AS store_date
FROM {{ ref('stg_raw__feature') }}

