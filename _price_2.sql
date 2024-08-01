with exchange_data_frame as (
    SELECT
        date_time datetime,
        exchange, 
        market,
        groupArray(close)[1] as price

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
)

select
    datetime as time,
    market,
    price as "_"
from exchange_data_frame
WHERE
	datetime >= $__fromTime

ORDER BY time ASC;
