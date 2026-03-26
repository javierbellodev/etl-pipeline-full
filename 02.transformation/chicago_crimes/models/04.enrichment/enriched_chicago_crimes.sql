{{ config(materialized='view') }}

-- Enriquecimiento: agregar campos derivados para análisis
SELECT
    *,
    -- Campos temporales
    EXTRACT(HOUR FROM crime_date) AS crime_hour,
    EXTRACT(DOW FROM crime_date) AS crime_day_of_week,
    EXTRACT(MONTH FROM crime_date) AS crime_month,
    EXTRACT(YEAR FROM crime_date) AS crime_year,

    -- Indicador de fin de semana
    CASE
        WHEN EXTRACT(DOW FROM crime_date) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    -- Período del día
    CASE
        WHEN EXTRACT(HOUR FROM crime_date) BETWEEN 6 AND 11 THEN 'Mañana'
        WHEN EXTRACT(HOUR FROM crime_date) BETWEEN 12 AND 17 THEN 'Tarde'
        WHEN EXTRACT(HOUR FROM crime_date) BETWEEN 18 AND 23 THEN 'Noche'
        ELSE 'Madrugada'
    END AS time_of_day,

    -- Indicador de hora pico (7-9 AM y 4-7 PM)
    CASE
        WHEN EXTRACT(HOUR FROM crime_date) BETWEEN 7 AND 9
          OR EXTRACT(HOUR FROM crime_date) BETWEEN 16 AND 19 THEN TRUE
        ELSE FALSE
    END AS is_rush_hour

FROM {{ ref('cleaned_chicago_crimes') }}
