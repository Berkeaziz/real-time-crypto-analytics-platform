BEGIN;

-- Silinecek duplicate kayıtları geri alınabilir şekilde saklanacak.
CREATE TABLE IF NOT EXISTS raw.trades_duplicate_backup
(LIKE raw.trades INCLUDING ALL);

WITH ranked_trades AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY source, symbol, trade_id
            ORDER BY id
        ) AS row_number
    FROM raw.trades
    WHERE
        source IS NOT NULL
        AND symbol IS NOT NULL
        AND trade_id IS NOT NULL
)
INSERT INTO raw.trades_duplicate_backup
SELECT trades.*
FROM raw.trades AS trades
JOIN ranked_trades
    ON ranked_trades.id = trades.id
WHERE ranked_trades.row_number > 1
ON CONFLICT (id) DO NOTHING;

-- Her trade için en küçük id'li ilk kaydı koruncak,
-- sonraki kopyaları silincek.
WITH ranked_trades AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY source, symbol, trade_id
            ORDER BY id
        ) AS row_number
    FROM raw.trades
    WHERE
        source IS NOT NULL
        AND symbol IS NOT NULL
        AND trade_id IS NOT NULL
)
DELETE FROM raw.trades AS trades
USING ranked_trades
WHERE
    trades.id = ranked_trades.id
    AND ranked_trades.row_number > 1;

-- Aynı business key'in tekrar yazılmasını engellenecek.
CREATE UNIQUE INDEX IF NOT EXISTS
    uq_raw_trades_source_symbol_trade_id
ON raw.trades (source, symbol, trade_id);

COMMIT;