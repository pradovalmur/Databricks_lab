{{ config(
    materialized='view'
) }}

select

    l.linkInvestidorTituloOperacaoHk,

    l.hubInvestidorHk,
    l.hubTituloHk,

    i.codigoDoInvestidor,

    t.tipoTitulo,

    s.dataDaOperacao,
    s.valorDaOperacao,
    s.tipoDaOperacao

from {{ source('silver', 'linkInvestidorTituloOperacao') }} l

left join {{ source('silver', 'hubInvestidor') }} i
    on l.hubInvestidorHk = i.hubInvestidorHk

left join {{ source('silver', 'hubTitulo') }} t
    on l.hubTituloHk = t.hubTituloHk

left join {{ ref('int_sat_operacao') }} s
    on l.linkInvestidorTituloOperacaoHk = s.linkInvestidorTituloOperacaoHk