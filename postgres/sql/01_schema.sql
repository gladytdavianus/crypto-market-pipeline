CREATE TABLE dim_coins (
    coin_id     VARCHAR(100) PRIMARY KEY,
    symbol      VARCHAR(100) NOT NULL,
    name        VARCHAR(300) NOT NULL,
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE fact_coin_prices (
    id              SERIAL PRIMARY KEY,
    coin_id         VARCHAR(100) NOT NULL REFERENCES dim_coins(coin_id),
    price_date      DATE NOT NULL,
    price_usd       NUMERIC(20, 8),
    market_cap_usd  NUMERIC(20, 2),
    volume_24h_usd  NUMERIC(20, 2),
    processed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (coin_id, price_date)
);


CREATE TABLE pipeline_run_log (
    id              SERIAL PRIMARY KEY,
    run_date        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stage           VARCHAR(50) NOT NULL
                    CHECK (stage IN ('extraction', 'transformation', 'loading')),
    status          VARCHAR(20) NOT NULL
                    CHECK (status IN ('running', 'success', 'failed')),
    rows_processed  INTEGER,
    error_message   TEXT
);
