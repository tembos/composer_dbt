{{ config(materialized='table') }}

WITH base AS (
    SELECT
        DATE(datetime) AS trade_date,
        open,
        high,
        low,
        close,
        volume,
        ROW_NUMBER() OVER (PARTITION BY DATE(datetime) ORDER BY datetime ASC) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY DATE(datetime) ORDER BY datetime DESC) AS rn_desc
    FROM {{ ref('stg_nvda') }}
)

SELECT
    trade_date,
    FIRST_VALUE(open) OVER (PARTITION BY trade_date ORDER BY rn_asc) AS daily_open,
    MAX(high) OVER (PARTITION BY trade_date) AS daily_high,
    MIN(low) OVER (PARTITION BY trade_date) AS daily_low,
    FIRST_VALUE(close) OVER (PARTITION BY trade_date ORDER BY rn_desc) AS daily_close,
    SUM(volume) OVER (PARTITION BY trade_date) AS total_volume
FROM base
GROUP BY trade_date, rn_asc, rn_desc, open, close, high, low, volume
