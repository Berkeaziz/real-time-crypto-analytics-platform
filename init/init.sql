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