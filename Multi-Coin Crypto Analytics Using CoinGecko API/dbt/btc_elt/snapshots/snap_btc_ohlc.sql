{% snapshot snap_btc_ohlc %}

{{
  config(
    target_schema='snapshot',
    unique_key='business_key',
    strategy='check',
    check_cols=['open','high','low','close'],
    invalidate_hard_deletes=True
  )
}}

select
  timestamp,
  open,
  high,
  low,
  close,
  coin_id,
  date,
  -- compose a stable unique key for this row
  concat(coin_id, '|', to_varchar(timestamp)) as business_key
from {{ source('raw','coin_gecko_ohlc') }}
-- If you want to snapshot only BTC + ETH, uncomment this:
-- where coin_id in ('bitcoin', 'ethereum')

{% endsnapshot %}
