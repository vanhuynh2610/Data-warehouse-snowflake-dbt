{{
    config(
        materialized='incremental',
        alias ='dim_date',
        unique_key ='date_key',
        incremental_strategy='delete+insert'
    )
}}
WITH date_sequence AS (
    SELECT DATEADD(DAY, seq4(), '2010-01-01') AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 50000))
    WHERE DATEADD(DAY, seq4(), '2010-01-01') <= CURRENT_DATE()
)

SELECT
    TO_NUMBER(TO_CHAR(date, 'YYYYMMDD')) AS date_key,
    date,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(MONTH FROM date) AS month,
    TO_CHAR(date, 'Month') AS month_name,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
    TO_CHAR(date, 'Day') AS weekday_name,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    EXTRACT(WEEK FROM date) AS week_of_year
FROM date_sequence

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
