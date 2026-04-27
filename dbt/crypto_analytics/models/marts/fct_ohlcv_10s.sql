{{ config(
    materialized='table',
    schema='marts'
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
        WHEN close_price < open_price THEN 'bearish'
        ELSE 'neutral'
    END AS candle_type,

    CASE
        WHEN open_price IS NOT NULL AND open_price != 0
        THEN ((close_price - open_price) / open_price) * 100
        ELSE NULL
    END AS candle_return_pct,

    CASE
        WHEN volume IS NOT NULL AND trade_count IS NOT NULL AND trade_count != 0
        THEN volume / trade_count
        ELSE NULL
    END AS avg_trade_size,

    created_at

FROM {{ source('marts', 'ohlcv_10s') }}