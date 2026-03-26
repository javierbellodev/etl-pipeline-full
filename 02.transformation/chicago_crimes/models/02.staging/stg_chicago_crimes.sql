{{ config(materialized='view') }}

-- Staging: selección y tipado de columnas relevantes
-- DLT almacena id como VARCHAR, date como TIMESTAMP WITH TIME ZONE,
-- arrest/domestic como BOOLEAN, y lat/lng como VARCHAR
SELECT
    TRY_CAST(id AS BIGINT) AS crime_id,
    case_number,
    CAST(date AS TIMESTAMP) AS crime_date,
    block,
    iucr,
    primary_type,
    description,
    location_description,
    arrest,
    domestic,
    beat,
    district,
    ward,
    community_area,
    fbi_code,
    TRY_CAST(x_coordinate AS DOUBLE) AS x_coordinate,
    TRY_CAST(y_coordinate AS DOUBLE) AS y_coordinate,
    TRY_CAST(year AS INTEGER) AS year,
    CAST(updated_on AS TIMESTAMP) AS updated_on,
    TRY_CAST(latitude AS DOUBLE) AS latitude,
    TRY_CAST(longitude AS DOUBLE) AS longitude
FROM {{ ref('src_chicago_crimes') }}
