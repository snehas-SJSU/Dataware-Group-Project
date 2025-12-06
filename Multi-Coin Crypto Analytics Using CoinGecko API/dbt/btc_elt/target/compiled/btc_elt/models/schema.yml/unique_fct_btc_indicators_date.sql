
    
    

select
    date as unique_field,
    count(*) as n_records

from USER_DB_PEACOCK.analytics.fct_btc_indicators
where date is not null
group by date
having count(*) > 1


