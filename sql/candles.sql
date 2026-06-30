-- 1-minute OHLC candles, one row per (product, minute bucket)
CREATE TABLE IF NOT EXISTS candles_1m (
    product_id   TEXT           NOT NULL,
    bucket_start TIMESTAMPTZ    NOT NULL,
    open         NUMERIC(38,18) NOT NULL,
    high         NUMERIC(38,18) NOT NULL,
    low          NUMERIC(38,18) NOT NULL,
    close        NUMERIC(38,18) NOT NULL,
    volume       NUMERIC(38,18) NOT NULL,
    trade_count  BIGINT         NOT NULL,
    PRIMARY KEY (product_id, bucket_start)
);

-- 1-hour OHLC candles, one row per (product, hour bucket)
CREATE TABLE IF NOT EXISTS candles_1h (
    product_id   TEXT           NOT NULL,
    bucket_start TIMESTAMPTZ    NOT NULL,
    open         NUMERIC(38,18) NOT NULL,
    high         NUMERIC(38,18) NOT NULL,
    low          NUMERIC(38,18) NOT NULL,
    close        NUMERIC(38,18) NOT NULL,
    volume       NUMERIC(38,18) NOT NULL,
    trade_count  BIGINT         NOT NULL,
    PRIMARY KEY (product_id, bucket_start)
);