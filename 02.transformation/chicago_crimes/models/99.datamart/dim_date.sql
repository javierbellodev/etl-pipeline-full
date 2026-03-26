{{ config(materialized='table') }}

-- Dimensión: Calendario
WITH date_range AS (
    SELECT CAST(RANGE AS DATE) AS date_key
    FROM RANGE(DATE '2024-01-01', DATE '2027-01-01', INTERVAL 1 DAY)
)

SELECT
    date_key AS id,
    DAYOFYEAR(date_key) AS day_of_year,
    YEARWEEK(date_key) AS week_key,
    WEEKOFYEAR(date_key) AS week_of_year,
    DAYOFWEEK(date_key) AS day_of_week,
    DAYNAME(date_key) AS day_name,
    MONTH(date_key) AS month_of_year,
    DAYOFMONTH(date_key) AS day_of_month,
    MONTHNAME(date_key) AS month_name,
    QUARTER(date_key) AS quarter_of_year,
    CAST(YEAR(date_key) AS INT) AS year_key,
    CASE WHEN DAYOFWEEK(date_key) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_range
