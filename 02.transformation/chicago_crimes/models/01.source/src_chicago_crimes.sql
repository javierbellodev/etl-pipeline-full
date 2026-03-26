{{ config(materialized='view') }}

SELECT * FROM {{ source('raw_crimes', 'crimes') }}
