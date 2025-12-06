-- models/input/stg_btc_market.sql
-- Staging for daily market data (multi-coin by default)
-- If coin_id var = 'ALL', keep all coins (recommended default)
-- If coin_id is passed (e.g., 'bitcoin'), filter accordingly.



SELECT
    timestamp,
    price,
    market_cap,
    volume,
    coin_id,
    date
FROM USER_DB_PEACOCK.raw.coin_gecko_market_daily
