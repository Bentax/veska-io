with exchange_data_frame as (
    SELECT
        date_time datetime,
        exchange, 
        market,
        groupArray(sell_count)[1] + groupArray(buy_count)[1] as price

    from (
      SELECT
            *
        from liquidations_1h 
        where 
            market in (${markets})
            AND exchange = '${exchange}'
            AND date_time >= date_sub(hour,2*${window_size},$__fromTime)
            AND date_time <=  $__toTime 
        order by date_time desc, updated_at desc
    )

    group by date_time, exchange, market
),

pre_load as (
    select
        datetime,
        market,
        price as sum_cur
    from  exchange_data_frame
)

select
    datetime as time,
    market,
    sum_cur as "_"
from pre_load
order by time asc;
