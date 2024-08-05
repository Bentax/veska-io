with exchange_data_frame as (
    SELECT
        toDateTime(agg_timestamp/1000) as datetime,
        exchange, 
        market,
        groupArray(volume_base_buy_taker)[1] AS vbbt,
        groupArray(volume_base_sell_taker)[1] AS vbst
    from aggregates_1h

    where
        market in (${market})
        AND exchange = '${exchange}'
        AND toDateTime(agg_timestamp/1000) >= date_sub(hour,2*${window_size},$__fromTime)
        AND toDateTime(agg_timestamp/1000) <=  $__toTime 
        
    group by agg_timestamp, exchange, market
),

exchange_data_frame_by_interval as (
    SELECT
        exchange,
        market,
        sum((vbbt-vbst)/(greatest(vbbt, vbst))) as __volume_base,
        datetime as grouping_datetime

    FROM
        exchange_data_frame
    GROUP BY grouping_datetime, exchange, market
)

select
    grouping_datetime as time,
    market,
    anyLast(__volume_base) OVER (PARTITION BY market,exchange ORDER BY grouping_datetime ASC Range BETWEEN toUInt64((${window_size}-1)*60*60) PRECEDING AND CURRENT ROW) AS "_"
from  exchange_data_frame_by_interval

order by time asc;
