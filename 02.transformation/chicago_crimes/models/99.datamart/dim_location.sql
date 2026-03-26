{{ config(materialized='table') }}

-- Dimensión: Ubicaciones
SELECT DISTINCT
    location_description,
    district,
    ward,
    community_area,
    beat
FROM {{ ref('cleaned_chicago_crimes') }}
WHERE location_description IS NOT NULL
ORDER BY district, location_description
