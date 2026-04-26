CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.trades (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50),
    symbol VARCHAR(20) NOT NULL,
    trade_id BIGINT,
    price NUMERIC(18,8) NOT NULL,
    quantity NUMERIC(18,8) NOT NULL,
    trade_time TIMESTAMPTZ NOT NULL,
    is_buyer_maker BOOLEAN,
    source VARCHAR(50),
    ingested_at TIMESTAMPTZ NOT NULL,
    event_time TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS marts.ohlcv_10s (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(18,8),
    high_price NUMERIC(18,8),
    low_price NUMERIC(18,8),
    close_price NUMERIC(18,8),
    volume NUMERIC(18,8),
    trade_count BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, window_start, window_end)
);