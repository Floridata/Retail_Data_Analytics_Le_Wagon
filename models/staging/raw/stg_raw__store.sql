with 

source as (

    select * from {{ source('raw', 'store') }}

),

renamed as (

    select
        store,
        type,
        size

    from source

)

select * from renamed
