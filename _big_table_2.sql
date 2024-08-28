with pre_load as (
    select
        datetime, exchange, market, 
        max(price) as price, max(volume_token) as volume_token, max(volume_usd) as volume_usd,
        max(open_interest) as open_interest, 
        max(buy_liquidation) as buy_liquidation, max(sell_liquidation) as sell_liquidation,
        max(buy_liquidation_val) as buy_liquidation_val, max(sell_liquidation_val) as sell_liquidation_val,
        max(rate) as rate,
        max(low_price) as low_price, max(high_price) as high_price
    from (
        (
            SELECT
                date_add(hour,if(toInt8(dateName('hour',toDateTime(agg_timestamp/1000)))%IF('${exchange}'='dydx', 1, 8) == 0,0,IF('${exchange}'='dydx', 1, 8)),
                        toStartOfInterval(toDateTime(agg_timestamp/1000), interval IF('${exchange}'='dydx', 1, 8) hour)) as datetime, 
                exchange, market,
                toDecimal32(any(if(funding_rate != 0, funding_rate*100, NULL)), 8) as rate,
                anyLast(price_open) as price,
                anyLast(volume_base) as volume_token, 
                anyLast(volume_quot) as volume_usd, 
                anyLast(oi_open) as open_interest, 
                anyLast(liquidations_longs_count) as buy_liquidation, 
                anyLast(liquidations_shorts_count) as sell_liquidation,
                anyLast(liquidations_longs_quot_volume) as buy_liquidation_val,
                anyLast(liquidations_shorts_quot_volume) as sell_liquidation_val,
                anyLast(price_high) as high_price,
                anyLast(price_low) as low_price
            from public.aggregates_1h
            where
                exchange = '${exchange}'
                AND market in (${markets})
                AND toDateTime(agg_timestamp/1000) between date_sub(hour,5*${window_size},$__toTime) AND $__toTime 
            group by datetime, exchange, market;
        )
        union all
        (
            select 
                datetime, exchange, market, 
                null as rate, max(open) as price,
                max(volume_token) as volume_token, max(volume_usd) as volume_usd, max(open_interest_open) as open_interest,
                null as buy_liquidation,
                null as sell_liquidation,
                null as buy_liquidation_val,
                null as sell_liquidation_val,
                max(high) as high_price,
                min(low) as low_price
            from price_stream
            where 
                datetime >= date_sub(hour,5*${window_size},$__toTime)
                and exchange = '${exchange}'
                and market in (${markets})
            group by datetime, exchange, market
        )
        union all
        (
            select 
                datetime, exchange, market, 
                null as rate, null as price,
                null as volume_token, null as volume_usd, null as open_interest, 
                max(buy_liquidation_number) as buy_liquidation, max(sell_liquidation_number) as sell_liquidation,
                max(buy_liquidation_value) as buy_liquidation_val,max(sell_liquidation_value) as sell_liquidation_val,
                null as high_price,
                null as low_price
            from liquidation_stream
            where 
                datetime >= date_sub(hour,5*${window_size},$__toTime)
                and exchange = '${exchange}'
                and market in (${markets})
            group by datetime, exchange, market
        )
    )
    group by datetime, exchange, market
), t0 as (
    select 
        market, datetime, rate, price,
        price_start, 
        
        sum1, sum1-sum2 as sum_diff1, 
        avg1, avg1-avg2 as avg_diff1, 


        volume_token_sum1, volume_usd_sum1, open_interest, sell_liquidation, buy_liquidation,

        price_start, 
        ((price_sum1-price_sum2)/price_sum2)*100 as price_wind_diff,
        ((open_interest-open_interest_start)/open_interest_start)*100 as open_interest_change,

        if(volume_token_sum1=0 and volume_token_sum2=0,0,if(volume_token_sum1>0 and volume_token_sum2=0,100,((volume_token_sum1-volume_token_sum2)/abs(volume_token_sum2))*100)) as vol_token_wind_diff,
        if(volume_usd_sum1=0 and volume_usd_sum2=0,0,if(volume_usd_sum1>0 and volume_usd_sum2=0,100,((volume_usd_sum1-volume_usd_sum2)/abs(volume_usd_sum2))*100)) as vol_usd_wind_diff,
        if(open_interest_sum1=0 and open_interest_sum2=0,0,if(open_interest_sum1>0 and open_interest_sum2=0,100,((open_interest_sum1-open_interest_sum2)/abs(open_interest_sum2))*100)) as open_int_wind_diff,
        buy_liquidation_sum1-buy_liquidation_sum2 as buy_liquid_wind_diff,
        sell_liquidation_sum1-sell_liquidation_sum2 as sell_liquid_wind_diff,
        buy_liquidation_sum1,sell_liquidation_sum1,
        buy_liquidation_val_sum1,sell_liquidation_val_sum1,
        buy_liquidation_token_sum1,sell_liquidation_token_sum1

    from (
        SELECT 
            *,
            sum(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as sum1,
            sum(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as sum2,

            avg(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as avg1,
            avg(rate) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as avg2,

            first_value(price) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as price_start,
            sum(price) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as price_sum1,
            sum(price) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as price_sum2,

            first_value(open_interest) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as open_interest_start,
            sum(open_interest) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as open_interest_sum1,
            sum(open_interest) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as open_interest_sum2,

            sum(volume_token) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as volume_token_sum1,
            sum(volume_token) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as volume_token_sum2,

            sum(volume_usd) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as volume_usd_sum1,
            sum(volume_usd) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as volume_usd_sum2,
            
            sum(buy_liquidation) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as buy_liquidation_sum1,
            sum(buy_liquidation) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as buy_liquidation_sum2,
            
            sum(sell_liquidation) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as sell_liquidation_sum1,
            sum(sell_liquidation) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)*2-1)*3600) preceding and toUInt64((($window_size))*3600) preceding) as sell_liquidation_sum2,

            sum(buy_liquidation_val) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as buy_liquidation_val_sum1,
            sum(buy_liquidation_val/high_price) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as buy_liquidation_token_sum1,

            sum(sell_liquidation_val) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as sell_liquidation_val_sum1,
            sum(sell_liquidation_val/low_price) over (partition by market, exchange order by datetime asc range between toUInt64((($window_size)-1)*3600) preceding and current row) as sell_liquidation_token_sum1
        from pre_load
    )
), stats as (
    select 
        datetime, market, exchange, rate, 
        avg(rate * 100) over (partition by market, exchange order by toDate(datetime) asc range between 12*30 preceding and current row) as avg_ref_12,
        avg(rate * 100) over (partition by market, exchange order by toDate(datetime) asc range between 3*30 preceding and current row) as avg_ref_3,
        avg(rate * 100) over (partition by market, exchange order by toDate(datetime) asc range between 6*30 preceding and current row) as avg_ref_6
    from funding_stream
    where price = 0 and  market in (${markets}) and exchange = '${exchange}' 
),
max_date as (
    select 
        max(datetime) as max_price_dt_1, -- последний прайс дейт
        if(date_trunc('hour',$__toTime) > max_price_dt_1, 
            max_price_dt_1,
            toStartOfInterval(date_trunc('hour',$__toTime), interval if('${exchange}'='dydx',1,8) hour)
        ) as  max_price_dt -- смысл в том чтобы найти самый свежий прайс или на конкретную дату или самый свежий 
    from price_stream 
    where exchange = '${exchange}'
)
SELECT 
    t0.* except(price_start,datetime, avg_diff1, avg1),

    s.avg_ref_12 * if('${exchange}'!='dydx',${window_size}/8,${window_size}) as avg_calc_sum_12, 
    if(sum1=0 and avg_calc_sum_12,0,if(sum1>0 and avg_calc_sum_12=0,1,((sum1-avg_calc_sum_12)/abs(avg_calc_sum_12))))*100 as sum__avgref_diff_12,
    s.avg_ref_3 * if('${exchange}'!='dydx',${window_size}/8,${window_size}) as avg_calc_sum_3, 
    if(sum1=0 and avg_calc_sum_3,0,if(sum1>0 and avg_calc_sum_3=0,1,( (sum1-avg_calc_sum_3)/abs(avg_calc_sum_3))))*100  as sum__avgref_diff_3,
    s.avg_ref_6 * if('${exchange}'!='dydx',${window_size}/8,${window_size}) as avg_calc_sum_6, 
    if(sum1=0 and avg_calc_sum_6,0,if(sum1>0 and avg_calc_sum_6=0,1,( (sum1-avg_calc_sum_6)/abs(avg_calc_sum_6))))*100  as sum__avgref_diff_6,

    ((t0.price - t0.price_start)/t0.price_start)*100 as price_change,
    t0.market as market2
FROM t0
left join stats s on s.market = t0.market and s.datetime = t0.datetime 
where t0.datetime = (select max_price_dt from max_date);
