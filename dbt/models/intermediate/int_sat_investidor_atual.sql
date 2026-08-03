{{ config(
    materialized='view'
) }}

with sat_investidor as (

    select *

    from {{ source('silver', 'satInvestidor') }}

),

latest_satellite as (

    select
        *,
        row_number() over (
            partition by hubInvestidorHk
            order by loadTs desc
        ) as rn

    from sat_investidor

)

select

    *

from latest_satellite

where rn = 1