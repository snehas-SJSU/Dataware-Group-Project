{% snapshot snap_btc_market %}

{{
  config(
    target_schema='snapshot',
    unique_key='business_key',
    strategy='check',
    check_cols=['price','market_cap','volume'],
    invalidate_hard_deletes=True
  )
}}

select
  timestamp,
  price,
  market_cap,
  volume,
  coin_id,
  date,
  -- Stable unique key per coin + timestamp
  concat(coin_id, '|', to_varchar(timestamp)) as business_key
from {{ source('raw','coin_gecko_market_daily') }}
-- If you want to snapshot only BTC + ETH, uncomment this:
-- where coin_id in ('bitcoin', 'ethereum')

{% endsnapshot %}
