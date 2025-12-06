select
    coin_id,
    date,
    count(*) as cnt
from {{ ref('fct_btc_indicators') }}
group by 1,2
having count(*) > 1
