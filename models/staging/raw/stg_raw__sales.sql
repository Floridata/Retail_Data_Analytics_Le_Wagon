with 

source as (

    select * from {{ source('raw', 'sales') }}

),

renamed as (

    select
        store,
        dept,
        date,
        weekly_sales,
        isholiday

    from source

)

select * from renamed
