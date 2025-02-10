{{ config(materialized='view') }}

SELECT
    SAFE_CAST(`datetime` AS DATETIME) as `datetime`,
    SAFE_CAST(open AS FLOAT64) as open,
    SAFE_CAST(high AS FLOAT64) as high,
    SAFE_CAST(low AS FLOAT64) as low,
    SAFE_CAST(close AS FLOAT64) as close,
    SAFE_CAST(volume AS INT64) as volume,
    JSON_QUERY(meta, '$') as meta,
    SAFE_CAST(ingestion_datetime AS DATETIME) as ingestion_datetime,
    batch_id

FROM {{ source('stocks_raw', 'source_nvda') }}
