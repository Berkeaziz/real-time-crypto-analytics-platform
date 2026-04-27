{{ config(
    materialized = "view",
    schema = "staging"
)}}

SELECT 
    id,
    event_type,
    symbol,
    trade_id,

    price,
    quantity,

    trade_time,
    event_time,
    ingested_at,

    is_buyer_maker,
    source,

    DATE_TRUNC('second',trade_time) AS trade_time_second,
    DATE_TRUNC('minute',trade_time) AS trade_time_minute,

    ingested_at - event_time  AS ingestion_delay

    FROM {{source ("raw","trades")}}

    WHERE 
        symbol IS NOT NULL
        AND price IS NOT NULL
        AND quantity IS NOT NULL
        AND trade_time IS NOT NULL
        AND event_time IS NOT NULL
        AND ingested_at IS NOT NULL