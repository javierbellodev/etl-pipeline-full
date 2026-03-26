{{ config(materialized='view') }}

-- Modelo estructural pre-fact: preparar campos para la tabla de hechos
SELECT
    crime_id,
    case_number,
    crime_date,
    crime_date::DATE AS crime_date_key,
    STRFTIME(crime_date, '%H:%M:%S') AS crime_time,
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
    latitude,
    longitude,
    crime_hour,
    crime_day_of_week,
    crime_month,
    crime_year,
    is_weekend,
    time_of_day,
    is_rush_hour
FROM {{ ref('enriched_chicago_crimes') }}
