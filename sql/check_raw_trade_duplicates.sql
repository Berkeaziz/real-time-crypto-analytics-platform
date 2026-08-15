SELECT
    source,
    symbol,
    trade_id,
    COUNT(*) AS duplicate_count
FROM raw.trades
GROUP BY
    source,
    symbol,
    trade_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;