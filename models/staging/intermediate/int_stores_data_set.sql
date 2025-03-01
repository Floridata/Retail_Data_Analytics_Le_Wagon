SELECT
Store,
Type,
Size as Size_sqf,
ROUND(Size *  0.092903,2) as Size_sqm
FROM {{ ref('stg_raw__store') }}