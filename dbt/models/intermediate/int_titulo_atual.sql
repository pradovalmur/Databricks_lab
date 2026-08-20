{{ config(
    materialized='view'
) }}

select

    hubTituloHk,
    tipoTitulo

from {{ source('silver', 'hubTitulo') }}