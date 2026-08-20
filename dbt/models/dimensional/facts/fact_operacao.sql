{{ config(
    materialized='table'
) }}

select

    b.hubInvestidorHk as investidorKey,
    b.hubTituloHk as tituloKey,

    b.totalOperacoes,
    b.totalDiasOperados,
    b.valorTotalOperado,
    b.primeiraOperacao,
    b.ultimaOperacao,

    current_timestamp() as factLoadTs,
    current_date() as factLoadDate

from {{ ref('bridge_investidor_titulo') }} b