-- models/input/stg_btc_market.sql
-- Staging for daily market data (multi-coin by default)
-- If coin_id var = 'ALL', keep all coins (recommended default)
-- If coin_id is passed (e.g., 'bitcoin'), filter accordingly.

{% set coin_filter = var("coin_id", "ALL") %}

SELECT
    timestamp,
    price,
    market_cap,
    volume,
    coin_id,
    date
FROM {{ source('raw', 'coin_gecko_market_daily') }}
{% if coin_filter != 'ALL' %}
WHERE coin_id = '{{ coin_filter }}'
{% endif %}
