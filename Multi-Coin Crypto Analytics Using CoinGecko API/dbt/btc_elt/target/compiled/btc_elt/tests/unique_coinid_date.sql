select
    coin_id,
    date,
    count(*) as cnt
from USER_DB_PEACOCK.analytics.fct_btc_indicators
group by 1,2
having count(*) > 1