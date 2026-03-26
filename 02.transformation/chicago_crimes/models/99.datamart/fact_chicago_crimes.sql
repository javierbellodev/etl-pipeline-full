{{ config(materialized='table') }}

-- Tabla de hechos final: Chicago Crimes
WITH _temp_data AS (
    SELECT
        *,
        NOW() AS audit_row_insert,
        'chicago_crimes_etl' AS audit_process_id
    FROM {{ ref('stg_fact_chicago_crimes') }}
)

SELECT * FROM _temp_data
