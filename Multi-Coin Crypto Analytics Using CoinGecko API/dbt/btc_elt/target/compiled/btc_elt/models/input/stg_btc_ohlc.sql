-- models/input/stg_btc_ohlc.sql
-- Multi-coin OHLC staging (default: ALL COINS)
-- If user passes coin_id var, we filter, otherwise return all



SELECT
    timestamp,
    open,
    high,
    low,
    close,
    coin_id,
    date
FROM USER_DB_PEACOCK.raw.coin_gecko_ohlc
