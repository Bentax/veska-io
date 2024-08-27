with pre_load as (
    SELECT
        grouping_datetime as datetime,
        *
    FROM (
        SELECT
            date_add(
                hour,
                if(toInt8(dateName('hour',toDateTime(agg_timestamp/1000)))%IF('${exchange}'='dydx', 1, 8) == 0,0,IF('${exchange}'='dydx', 1, 8)),
                toStartOfInterval(toDateTime(agg_timestamp/1000), interval IF('${exchange}'='dydx', 1, 8) hour) 
            ) as grouping_datetime,
            exchange, 
            market,
            
            toDecimal32(any(if(funding_rate != 0, funding_rate*100, NULL)), 8) as rate,

            any(if(price_open != 0, price_open, NULL)) as price,
            min(if(price_low != 0, price_low, NULL)) as min_price,
            max(if(price_high != 0, price_high, NULL)) as max_price

        from public.aggregates_1h

        where
            exchange = '${exchange}'
            AND market in (${markets})
            AND toDateTime(agg_timestamp/1000) between date_sub(hour,5*${window_size},$__toTime) AND  $__toTime 
        group by agg_timestamp, exchange, market
    )
), t0 as (
    select 
        market, datetime, rate, price1,
        sum1, 
        sum1-sum2 as sum_diff1, 
        sum_diff1-(sum2-sum3) as sum_diff2,
        avg1, 
        avg1-avg2 as avg_diff1, 
        avg_diff1-(avg2-avg3) as avg_diff2,
        mean1,
        mean1-mean2 as mean_diff1, 
        mean_diff1-(mean2-mean3) as mean_diff2

    from
    (SELECT 
      *,

      first_value(price) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as price1,
      sum(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as sum1,
      sum(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as sum2,
      sum(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*3-1)*3600) preceding and toUInt64((($window_size)*2)*3600) preceding) as sum3,

      avg(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as avg1,
      avg(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as avg2,
      avg(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*3-1)*3600) preceding and toUInt64((($window_size)*2)*3600) preceding) as avg3,

      median(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as mean1,
      median(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as mean2,
      median(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*3-1)*3600) preceding and toUInt64((($window_size)*2)*3600) preceding) as mean3
    from pre_load
    order by exchange, datetime desc)
), stats as (
    select 
        toDateTime(agg_timestamp/1000) as datetime, market, exchange, funding_rate, 
        avg(funding_rate * 100) over (partition by market, exchange order by toDate(datetime) asc range between 12*30 preceding and current row) as avg_ref_12,
        median(funding_rate * 100) over (partition by market, exchange order by toDate(datetime) asc range between 12*30 preceding and current row) as mean_ref_12,
        avg(funding_rate * 100) over (partition by market, exchange order by toDate(datetime) asc range between 3*30 preceding and current row) as avg_ref_3,
        median(funding_rate  * 100) over (partition by market, exchange order by toDate(datetime) asc range between 3*30 preceding and current row) as mean_ref_3,
        avg(funding_rate  * 100) over (partition by market, exchange order by toDate(datetime) asc range between 6*30 preceding and current row) as avg_ref_6,
        median(funding_rate * 100) over (partition by market, exchange order by toDate(datetime) asc range between 6*30 preceding and current row) as mean_ref_6
    from public.aggregates_1h
    where price_open = 0 and  exchange = '${exchange}' and market in (${markets})
),
max_date as (
    select 
        max(toDateTime(agg_timestamp/1000)) as m_date, 
        if(date_trunc('hour',$__toTime) > m_date, 
            m_date,
            toStartOfInterval(date_trunc('hour',$__toTime), interval if('${exchange}'='dydx',1,8) hour)
        ) as to_filtDate,
        max(if(price_open!=0, toDateTime(agg_timestamp/1000),null)) as max_price_dt_1,
        if(date_trunc('hour',$__toTime) > max_price_dt_1, 
            max_price_dt_1,
            toStartOfInterval(date_trunc('hour',$__toTime), interval if('${exchange}'='dydx',1,8) hour)
        ) as  max_price_dt
    from public.aggregates_1h 
    where exchange = '${exchange}'
),
stats2 as (
    select  market as m2, max(price_open) as now_price
    from public.aggregates_1h
    where price_open != 0 and exchange = '${exchange}' and toDateTime(agg_timestamp/1000) = (select max_price_dt from max_date)
    group by market
)
SELECT 
    t0.* except(price1,datetime,mean_diff1, avg_diff1, mean_diff2, mean1, avg1, avg_diff2),

    s.avg_ref_12 * if('${exchange}'!='dydx',${window_size}/8,${window_size}) as avg_calc_sum_12, 
    if(sum1=0 and avg_calc_sum_12,0,if(sum1>0 and avg_calc_sum_12=0,1,((sum1-avg_calc_sum_12)/abs(avg_calc_sum_12))))*100 as sum__avgref_diff_12,
    s.mean_ref_12 * if('${exchange}'!='dydx',${window_size}/8,${window_size}) as mean_calc_sum_12, 
    if(sum1=0 and mean_calc_sum_12,0,if(sum1>0 and mean_calc_sum_12=0,1,( (sum1-mean_calc_sum_12)/abs(mean_calc_sum_12))))*100  as sum__meanref_diff_12,

    s.avg_ref_3 * if('${exchange}'!='dydx',${window_size}/8,${window_size}) as avg_calc_sum_3, 
    if(sum1=0 and avg_calc_sum_3,0,if(sum1>0 and avg_calc_sum_3=0,1,( (sum1-avg_calc_sum_3)/abs(avg_calc_sum_3))))*100  as sum__avgref_diff_3,
    s.mean_ref_3 * if('${exchange}'!='dydx',${window_size}/8,${window_size}) as mean_calc_sum_3, 
    if(sum1=0 and mean_calc_sum_3,0,if(sum1>0 and mean_calc_sum_3=0,1,( (sum1-mean_calc_sum_3)/abs(mean_calc_sum_3))))*100  as sum__meanref_diff_3,

    s.avg_ref_6 * if('${exchange}'!='dydx',${window_size}/8,${window_size}) as avg_calc_sum_6, 
    if(sum1=0 and avg_calc_sum_6,0,if(sum1>0 and avg_calc_sum_6=0,1,( (sum1-avg_calc_sum_6)/abs(avg_calc_sum_6))))*100  as sum__avgref_diff_6,
    s.mean_ref_6 * if('${exchange}'!='dydx',${window_size}/8,${window_size}) as mean_calc_sum_6,
    if(sum1=0 and mean_calc_sum_6,0,if(sum1>0 and mean_calc_sum_6=0,1,( (sum1-mean_calc_sum_6)/abs(mean_calc_sum_6))))*100  as sum__meanref_diff_6,

    ((s2.now_price - t0.price1)/t0.price1)*100 as price_change,
    now_price,
    t0.market as market2
FROM t0
left join stats s on s.market = t0.market and s.datetime = t0.datetime 
left join stats2 s2 on s2.m2 = t0.market
where t0.datetime = (select to_filtDate from max_date);
