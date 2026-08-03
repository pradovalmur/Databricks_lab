{{ config(
    materialized='table'
) }}

select

    h.hubInvestidorHk,
    h.codigoDoInvestidor,

    s.dataDeAdesao,
    s.estadoCivil,
    s.genero,
    s.profissao,
    s.idade,
    s.ufDoInvestidor,
    s.cidadeDoInvestidor,
    s.paisDoInvestidor,
    s.situacaoDaConta,
    s.operou12Meses,

    s.loadTs,
    s.loadDate,

    current_timestamp() as pitLoadTs,
    current_date() as pitLoadDate

from {{ source('silver', 'hubInvestidor') }} h

left join {{ ref('int_sat_investidor_atual') }} s
    on h.hubInvestidorHk = s.hubInvestidorHk