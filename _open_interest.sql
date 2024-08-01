with exchange_data_frame as (
    SELECT
        date_time datetime,
        exchange, 
        market,
        groupArray(open_interest_open)[1] * groupArray(close)[1] as price

    from (
      SELECT
            *
        from candles_1h 
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
        sum(price) OVER (PARTITION BY market ORDER BY datetime ASC RANGE BETWEEN toUInt64((${window_size}-1)*60*60) PRECEDING AND CURRENT ROW) AS sum_cur
    from  exchange_data_frame
)

select
    datetime as time,
    market,
    sum_cur as "_"
from pre_load
order by time asc;
