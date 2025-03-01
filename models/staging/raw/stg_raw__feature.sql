with 

source as (

    select * from {{ source('raw', 'feature') }}

),

renamed as (

    select
        store,
        date,
        temperature,
        fuel_price,
        markdown1,
        markdown2,
        markdown3,
        markdown4,
        markdown5,
        cpi,
        unemployment,
        isholiday

    from source

)

select * from renamed
