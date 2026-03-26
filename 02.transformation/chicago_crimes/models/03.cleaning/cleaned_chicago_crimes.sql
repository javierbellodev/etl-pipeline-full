{{ config(materialized='view') }}

-- Limpieza: filtrar registros inválidos, nulos y duplicados
WITH cleaned AS (
    SELECT *
    FROM {{ ref('stg_chicago_crimes') }}
    WHERE crime_id IS NOT NULL
      AND crime_date IS NOT NULL
      AND primary_type IS NOT NULL
),

-- Validar coordenadas geográficas
valid_coords AS (
    SELECT *
    FROM cleaned
    WHERE (latitude IS NULL AND longitude IS NULL)
       OR (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
)

-- Eliminar registros duplicados por crime_id
SELECT DISTINCT ON (crime_id) *
FROM valid_coords
ORDER BY crime_id
