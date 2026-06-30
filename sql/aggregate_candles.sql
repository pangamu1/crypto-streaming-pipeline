INSERT INTO {table}
    (product_id, bucket_start, open, high, low, close, volume, trade_count)
SELECT
    product_id,
    date_trunc('{grain}', time)                                     AS bucket_start,
    (array_agg(price ORDER BY time ASC,  trade_id::bigint ASC ))[1] AS open,
    max(price)                                                      AS high,
    min(price)                                                      AS low,
    (array_agg(price ORDER BY time DESC, trade_id::bigint DESC))[1] AS close,
    sum(size)                                                       AS volume,
    count(*)                                                        AS trade_count
FROM trades
GROUP BY product_id, date_trunc('{grain}', time)
ON CONFLICT (product_id, bucket_start) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
    close = EXCLUDED.close, volume = EXCLUDED.volume, trade_count = EXCLUDED.trade_count;