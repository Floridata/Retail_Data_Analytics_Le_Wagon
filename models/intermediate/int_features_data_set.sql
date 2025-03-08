SELECT
  Store, 
  Date as Date_date, 
  Temperature as Temperature_Farenheit,
  ROUND((Temperature-32)*5/9, 2) AS Temperature_Celsius,
  Fuel_Price,
  COALESCE(SAFE_CAST(REPLACE(MarkDown1, ',', '.') AS FLOAT64), 0) AS Markdown1,
  COALESCE(SAFE_CAST(REPLACE(MarkDown2, ',', '.') AS FLOAT64), 0) AS Markdown2,
  COALESCE(SAFE_CAST(REPLACE(MarkDown3, ',', '.') AS FLOAT64), 0) AS Markdown3,
  COALESCE(SAFE_CAST(REPLACE(MarkDown4, ',', '.') AS FLOAT64), 0) AS Markdown4,
  COALESCE(SAFE_CAST(REPLACE(MarkDown5, ',', '.') AS FLOAT64), 0) AS Markdown5,
  COALESCE(SAFE_CAST(REPLACE(CPI, ',', '.') AS FLOAT64), 0) AS CPI,
  COALESCE(SAFE_CAST(REPLACE(unemployment, ',', '.') AS FLOAT64), 0) AS unemployment,
  CONCAT('Store ', CAST(store AS STRING),' - ', CAST(date AS STRING)) AS Store_date
FROM {{ ref('stg_raw__feature') }}

