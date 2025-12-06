-- models/input/stg_btc_ohlc.sql
-- Multi-coin OHLC staging (default: ALL COINS)
-- If user passes coin_id var, we filter, otherwise return all

{% set coin_filter = var("coin_id", "ALL") %}

SELECT
    timestamp,
    open,
    high,
    low,
    close,
    coin_id,
    date
FROM {{ source('raw', 'coin_gecko_ohlc') }}
{% if coin_filter != 'ALL' %}
WHERE coin_id = '{{ coin_filter }}'
{% endif %}
