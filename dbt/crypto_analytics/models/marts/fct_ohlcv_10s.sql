{{ config(
    materialized = "table",
    schema = "marts"
) }}

SELECT
    symbol,
    window_start,
    window_end,

    open_price,
    high_price,
    low_price,
    close_price,

    volume,
    trade_count,

    (high_price - low_price) AS price_range,
    (close_price - open_price) AS candle_body,

    CASE
        WHEN close_price > open_price THEN 'bullish'
        ELSE 'bearish'
    END AS candle_type,

    created_at

FROM {{ source('marts', 'ohlcv_10s') }}