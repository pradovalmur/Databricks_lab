{{ config(
    materialized='view'
) }}

select

    linkInvestidorTituloOperacaoHk,

    dataDaOperacao,

    cast(
        replace(valorDaOperacao, ',', '.')
        as double
    ) as valorDaOperacao,

    tipoDaOperacao

from {{ source('silver', 'satOperacao') }}