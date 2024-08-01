with exchange_data_frame as (
    SELECT
        date_time as datetime,
        exchange, 
        market,
        
        any(rate*100) as funding
    from funding_v2

    where
        market in (${markets})
        AND exchange = '${exchange}'
        AND date_time >= date_sub(hour,2*${window_size},$__fromTime)
        AND date_time <=  $__toTime 
        
    group by date_time, exchange, market
),

exchange_data_frame_by_interval as (
    SELECT
        exchange,
        market,
        sum(funding) as funding,
        datetime as grouping_datetime

    FROM
        exchange_data_frame
    GROUP BY grouping_datetime, exchange, market
)

select
    grouping_datetime as time,
    market,
    sum(funding) OVER (PARTITION BY market,exchange ORDER BY grouping_datetime ASC Range BETWEEN toUInt64((${window_size}-1)*60*60) PRECEDING AND CURRENT ROW) AS "_"
from  exchange_data_frame_by_interval

order by time asc
