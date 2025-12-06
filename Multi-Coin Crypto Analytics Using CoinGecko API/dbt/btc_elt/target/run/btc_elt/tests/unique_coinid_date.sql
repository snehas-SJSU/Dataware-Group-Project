select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      select
    coin_id,
    date,
    count(*) as cnt
from USER_DB_PEACOCK.analytics.fct_btc_indicators
group by 1,2
having count(*) > 1
      
    ) dbt_internal_test