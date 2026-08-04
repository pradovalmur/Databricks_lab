{{ config(
    materialized='table'
) }}

select

    hubInvestidorHk as investidorKey,

    codigoDoInvestidor,

    dataDeAdesao,

    estadoCivil,

    genero,

    profissao,

    idade,

    ufDoInvestidor,

    cidadeDoInvestidor,

    paisDoInvestidor,

    situacaoDaConta,

    operou12Meses,

    pitLoadTs,

    pitLoadDate

from {{ ref('pit_investidor') }}