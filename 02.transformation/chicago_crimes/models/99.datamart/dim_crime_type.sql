{{ config(materialized='table') }}

-- Dimensión: Tipos de crimen
SELECT DISTINCT
    primary_type,
    description
FROM {{ ref('cleaned_chicago_crimes') }}
WHERE primary_type IS NOT NULL
ORDER BY primary_type, description
