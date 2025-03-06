SELECT ST.Type, Size_sqf, Size_sqm, 
Temperature_Farenheit, Temperature_Celsius, Fuel_Price, Markdown1, Markdown2, Markdown3, Markdown4, Markdown5, CPI, Unemployment, 
S.Store, Dept,	S.Date_date, Weekly_Sales, IsHoliday, Store_dept_date



FROM {{ ref('int_sales_data_set') }} S

LEFT JOIN {{ ref('int_features_data_set') }} F

ON S.Store = F.Store AND S.Date_date = F.Date_date

LEFT JOIN {{ ref('int_stores_data_set') }} ST

ON S.Store = ST.Store