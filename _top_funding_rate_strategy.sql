with pre_query as (
    select
      IF (event = 'candles_1h', date_time - interval 1 hour, date_time) datetime,
      market,
      funding_rate,
      candles_price_open,
      candles_price_high,
      candles_price_low
    from exchanges_events
    where
        event = 'candles_1h' or event='funding'
        AND market in (${markets})
        AND exchange = '${exchange}'
        AND datetime <=  $__toTime
),

t0 as (
  select
    datetime,
    market,
    any(funding_rate) rate,
    any(candles_price_open) price,
    any(candles_price_high) max_price,
    any(candles_price_low) min_price

  from pre_query
  group by datetime, market 
  having price is not NULL and rate is not null
),

stats as (
    select if(from_time<=min_datetime,min_datetime,from_time) as real_fromTime
    from (
        select 
            min(datetime) as min_datetime, 
            toStartOfInterval(toDateTime($__fromTime), interval IF(True, 1, 8) hour) as from_time
        from 
            pre_query
     )
),


exchange_data_frame_by_interval as (
    select *,
        row_number() over( PARTITION BY start_time order by sum_ws desc) as rn_desc,
        row_number() over( PARTITION BY start_time order by sum_ws asc) as rn_asc,
        (dateDiff('hour',(select real_fromTime from stats),start_time)) as dd_diff
    from (
        select
            grouping_datetime as start_time,
            date_add(hour,${step_size}, grouping_datetime) as end_time,
            market,
            rate as start_rate,
            price as start_price, 
            sum(rate) OVER (PARTITION BY market ORDER BY grouping_datetime ASC Range BETWEEN toUInt64((${window_size}-1)*60*60) PRECEDING AND CURRENT ROW) AS "sum_ws",
            min(min_price) over (partition by market  order by grouping_datetime asc range between 1 following  and toUInt64((${step_size})*60*60) following) as min_price,
            max(max_price) over (partition by market  order by grouping_datetime asc range between 1 following  and toUInt64((${step_size})*60*60) following) as max_price,
            last_value(price) over (partition by market  order by grouping_datetime asc range between current row and toUInt64((${step_size})*60*60) following) as end_price
        from
            (SELECT
                market,
                sum(rate) as rate,
                min(min_price) as min_price,
                max(max_price) as max_price,
                avg(price) as price,
                date_add(
                    hour,
                    if(toInt8(dateName('hour',datetime))%IF(True, 1, 8) == 0,0,IF(True, 1, 8)),
                    toStartOfInterval(datetime, interval IF(True, 1, 8) hour) 
                ) as grouping_datetime
            FROM
                t0
            GROUP BY grouping_datetime, market
        )
        
        order by sum_ws desc
    ) where start_time >= (select real_fromTime from stats) and         sum_ws != 0
)
select 
    sum(rel_diff) as total, 
    sum(if(position = 'long',rel_diff,0)) as long_sum,  
    sum(if(position = 'short',rel_diff,0)) as short_sum
from (
    select * except(rn_desc, rn_asc, end_time), 
        if(end_time >= toDateTime(now()),toStartOfInterval(toDateTime(now()), interval IF('${exchange}'='dydx', 1, 8) hour) ,end_time) as end_time,
        if(rn_desc <= ${n_top}, 'long', 'short') as position,
        if(position = 'long', end_price-start_price,start_price-end_price) as abs_diff,
        if(position = 'long', (end_price-start_price)/start_price,(start_price-end_price)/start_price) as rel_diff1,
        rel_diff1 * 100 as rel_diff
    from exchange_data_frame_by_interval
    where 

        (rn_desc <= ${n_top} or rn_asc <= ${n_top})  
        and ((dd_diff) % (${step_size})) == 0 and start_time >= (select real_fromTime from stats) and end_time > start_time
)
