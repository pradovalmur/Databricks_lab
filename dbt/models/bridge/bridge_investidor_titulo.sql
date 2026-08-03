{{ config(
    materialized='table'
) }}

select

    hubInvestidorHk,

    hubTituloHk,

    codigoDoInvestidor,

    tipoTitulo,

    count(*) as totalOperacoes,

    count(distinct dataDaOperacao) as totalDiasOperados,

    sum(valorDaOperacao) as valorTotalOperado,

    min(dataDaOperacao) as primeiraOperacao,

    max(dataDaOperacao) as ultimaOperacao,

    current_timestamp() as bridgeLoadTs,

    current_date() as bridgeLoadDate

from {{ ref('int_operacoes_enriquecida') }}

group by

    hubInvestidorHk,

    hubTituloHk,

    codigoDoInvestidor,

    tipoTitulo