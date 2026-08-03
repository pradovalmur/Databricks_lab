{{ config(
    materialized='table'
) }}

select

    h.hubInvestidorHk,

    h.codigoDoInvestidor,

    s.nome,
    s.uf,
    s.cidade,
    s.perfilInvestidor,
    s.loadTs,

    current_timestamp() as pitLoadTs,

    current_date() as pitLoadDate

from {{ source('silver', 'hubInvestidor') }} h

left join {{ ref('int_sat_investidor_atual') }} s

    on h.hubInvestidorHk = s.hubInvestidorHk