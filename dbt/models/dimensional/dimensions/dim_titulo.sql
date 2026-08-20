{{ config(
    materialized='table'
) }}

select

    hubTituloHk as tituloKey,
    tipoTitulo

from {{ ref('int_titulo_atual') }}