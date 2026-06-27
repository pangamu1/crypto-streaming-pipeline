CREATE TABLE IF NOT EXISTS trades (
    product_id   TEXT          NOT NULL,
    trade_id     TEXT          NOT NULL,
    price        NUMERIC(38,18) NOT NULL,
    size         NUMERIC(38,18) NOT NULL,
    time         TIMESTAMPTZ   NOT NULL,
    side         TEXT          NOT NULL,
    ingested_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, trade_id)
);