with p_s as (
    select 
        datetime, exchange, market, 
        max(open) as price, min(low) as min_price, max(high) as max_price, 
        max(volume_token) as volume_token, max(volume_usd) as volume_usd, max(open_interest_open) as open_interest, 
        null as buy_liquidation, null as sell_liquidation, null as funding
    from price_stream
    where 
        datetime >= date_sub(hour,greatest(toInt16(if('${var1_ws}'='','48','${var1_ws}')),toInt16(if('${var2_ws}'='','48','${var2_ws}')))*2,$__fromTime) and datetime <= date_add(hour,2*${price_horizon},$__toTime)
        and exchange = '${exchange}'
        and market in (${markets})
        and event = 'dydx_candle'
    group by datetime, exchange, market
),
l_s as (
    select 
        datetime, exchange, market, 
        null as price, null as min_price, null as max_price,
        null as volume_token, null as volume_usd, null as open_interest,
        max(buy_liquidation_number) as buy_liquidation, max(sell_liquidation_number) as sell_liquidation,
        null as funding
    from liquidation_stream
    where 
        datetime >= date_sub(hour,greatest(toInt16(if('${var1_ws}'='','48','${var1_ws}')),toInt16(if('${var2_ws}'='','48','${var2_ws}')))*2,$__fromTime) and datetime <= date_add(hour,2*${price_horizon},$__toTime)
        and exchange = '${exchange}'
        and market in (${markets})
        and event = 'trades'
    group by datetime, exchange, market
),
f_s as (        
    select 
        datetime, exchange, market, 
        null as price1, null as min_price, null as max_price,
        null as volume_token, null as volume_usd, null as open_interest,
        null as buy_liquidation, null as sell_liquidation,
        max(rate*100) as funding
    from 
        funding_stream
    where datetime >= date_sub(hour,greatest(toInt16(if('${var1_ws}'='','48','${var1_ws}')),toInt16(if('${var2_ws}'='','48','${var2_ws}')))*2,$__fromTime) 
        AND datetime <=  date_add(hour,2*${price_horizon},$__toTime) 
        and exchange = '${exchange}'
        and market in (${markets})
        AND price = 0
    group by datetime, exchange, market
),base0 as (
    select
        datetime, exchange, market, 
        max(price) as price, max(min_price) as min_price, max(max_price) as max_price, max(volume_token) as volume_token, max(volume_usd) as volume_usd,
        max(open_interest) as open_interest, max(buy_liquidation) as buy_liquidation, max(sell_liquidation) as sell_liquidation,
        max(funding) as funding,
        NULL as price_diff, NULL as funding_diff, NULL as volume_token_diff, NULL as volume_usd_diff, NULL as open_interest_diff,
        NULL as buy_liquidation_diff, NULL as sell_liquidation_diff
    from (
    select * from p_s
    union all 
    select * from l_s
    union all
    select * from f_s
    )
    group by datetime, exchange, market
),  base as (
    select *, 
        CASE  
            when like('${var1}','funding%') then funding
            when like('${var1}','price%') then price
            when like('${var1}','volume_token%') then volume_token
            when like('${var1}','volume_usd%') then volume_usd
            when like('${var1}','open_interest%') then open_interest
            when like('${var1}','buy_liquidation%') then buy_liquidation
            when like('${var1}','sell_liquidation%') then sell_liquidation
        ELSE funding
        END as filter_value1,

        CASE  
            when like('${var2}','funding%') then funding
            when like('${var2}','price%') then price
            when like('${var2}','volume_token%') then volume_token
            when like('${var2}','volume_usd%') then volume_usd
            when like('${var2}','open_interest%') then open_interest
            when like('${var1}','buy_liquidation%') then buy_liquidation
            when like('${var1}','sell_liquidation%') then sell_liquidation
        ELSE funding
        END as filter_value2

    from base0
    where not (price = 0 and volume_token = 0 and datetime = date_sub(hour,0,date_trunc('hour',now())) )
),
calculations as (
    select 
        * ,
        abs(if(filter1_metric_lag>=filter1_metric,filter1_metric_lag-filter1_metric,filter1_metric-filter1_metric_lag)/filter1_metric_lag)*if(filter1_metric_lag>filter1_metric,-1,1) as metric1_change,
        if(notLike('${var1}','%_diff'),filter1_metric,metric1_change) as filter1_res,
        lagInFrame(filter1_res) over (partition by exchange, market order by datetime asc rows between unbounded PRECEDING and unbounded following) as filter1_res_lag
       
        ,
        abs(if(filter2_metric_lag>=filter2_metric,filter2_metric_lag-filter2_metric,filter2_metric-filter2_metric_lag)/filter2_metric_lag)*if(filter2_metric_lag>filter2_metric,-1,1) as metric2_change,
        if(notLike('${var2}','%_diff'),filter2_metric,metric2_change) as filter2_res,
        lagInFrame(filter2_res) over (partition by exchange, market order by datetime asc rows between unbounded PRECEDING and unbounded following) as filter2_res_lag

        ,
        abs(if(max_price>=price,max_price-price,price-max_price)/price)*if(max_price>=price,100,-100) as max_price_change,
        abs(if(min_price>=price,min_price-price,price-min_price)/price)*if(min_price>=price,100,-100) as min_price_change,
        (abs(price-end_price)/price) * if(price>=end_price,if('$position_type'=='LONG',-100,100),if('$position_type'=='LONG',100,-100)) as price_diff,
        date_add(hour,${price_horizon}, datetime) as end_time
        from (
        select * except(min_price, max_price),
            max(max_price) over (partition by exchange, market order by datetime range between 60*60 FOLLOWING and toUInt64((${price_horizon})*60*60)  FOLLOWING)  as max_price,
            min(min_price) over (partition by exchange, market order by datetime range between 60*60 FOLLOWING and toUInt64((${price_horizon})*60*60)  FOLLOWING)  as min_price,
            last_value(price) over (partition by exchange, market order by datetime range between 60*60 FOLLOWING and toUInt64((${price_horizon})*60*60)  FOLLOWING)  as end_price,

            sum(filter_value1) over (partition by exchange, market order by datetime asc range between toUInt64((toInt16(if('${var1_ws}'='','1','${var1_ws}'))-1)*60*60) PRECEDING and CURRENT ROW) as filter1_metric,
            sum(filter_value1) over (partition  by exchange, market order by datetime asc range between toUInt64((toInt16(if('${var1_ws}'='','1','${var1_ws}'))*2-1)*60*60) PRECEDING and toUInt64((toInt16(if('${var1_ws}'='','1','${var1_ws}')))*60*60) PRECEDING) as filter1_metric_lag,
            sum(filter_value2) over (partition by exchange, market order by datetime asc range between toUInt64((toInt16(if('${var2_ws}'='','1','${var2_ws}'))-1)*60*60) PRECEDING and CURRENT ROW) as filter2_metric,
            sum(filter_value2) over (partition  by exchange, market order by datetime asc range between toUInt64((toInt16(if('${var2_ws}'='','1','${var2_ws}'))*2-1)*60*60) PRECEDING and toUInt64((toInt16(if('${var2_ws}'='','1','${var2_ws}')))*60*60) PRECEDING) as filter2_metric_lag
        from base
    )

), default_vals as (
    select 
        market, 
        floor(min(filter1_res))-1 as filter1_res_min, ceil(max(filter1_res))+1 as filter1_res_max, 
        floor(min(filter2_res))-1 as filter2_res_min, ceil(max(filter2_res))+1 as filter2_res_max
    from calculations
    group by market
), 
final_t_1 as (
    select 
        *, 
        groupArray(cond_match) over(partition by exchange, market order by datetime asc rows between toUInt64((toInt16(if('${max_length}'='','1','${max_length}')))) PRECEDING and 1 PRECEDING) as arr_cond
    from  (
    select c.*, 
        (filter1_res between toFloat64(if('${var1_from}' = '', toString(d.filter1_res_min), '${var1_from}')) and toFloat64(if('${var1_to}' = '', toString(d.filter1_res_max), '${var1_to}'))) as filter1_cond, 
        (filter1_res_lag between toFloat64(if('${var1_from}' = '', toString(d.filter1_res_min), '${var1_from}')) and toFloat64(if('${var1_to}' = '', toString(d.filter1_res_max), '${var1_to}'))) as filter1_cond_lag

        , 
        (filter2_res between toFloat64(if('${var2_from}' = '', toString(d.filter2_res_min), '${var2_from}')) and toFloat64(if('${var2_to}' = '', toString(d.filter2_res_max), '${var2_to}'))) as filter2_cond, 
        (filter2_res_lag between toFloat64(if('${var2_from}' = '', toString(d.filter2_res_min), '${var2_from}')) and toFloat64(if('${var2_to}' = '', toString(d.filter2_res_max), '${var2_to}'))) as filter2_cond_lag,
        ('${var1_to}' = '' and '${var1_from}' = '') as f1_empty,
        ('${var2_to}' = '' and '${var2_from}' = '') as f2_empty,
        
        CASE
            when (filter1_cond  and filter2_cond) then True
            when (filter1_cond  and f2_empty) or (filter2_cond and f1_empty) then True
            when '${rel_type}'='OR' and ((filter1_cond and not f1_empty) or (filter2_cond and not f2_empty)) then True
        ELSE False
        END AS cond_match,
        CASE
            when (filter1_cond_lag  and filter2_cond_lag) then True
            when (filter1_cond_lag  and f2_empty) or (filter2_cond_lag and f1_empty) then True
            when '${rel_type}'='OR' and ((filter1_cond_lag and not f1_empty) or (filter2_cond_lag and not f2_empty)) then True
        ELSE False
        END AS cond_match_lag,
        IF( '${market_group}' != 'others',
            has((SELECT markets from market_groups where group = '${market_group}'), market),
            not has((SELECT markets from market_groups where group = '${market_group}'), market)
        ) as is_in_group
        from calculations c
    left join default_vals d on d.market = c.market
    )
    WHERE 
        (datetime >= $__fromTime and datetime <= $__toTime) 
        and ('${market_group}' = 'all' OR is_in_group)
),
final_t as (
    select 
        * except (cond_match_lag),
        (select min(datetime) as mdt from final_t_1) as mdt,
        if(cond_match = False and has(arr_cond,True),True,cond_match) as cond_match1,
        if(datetime = mdt, False, if(has(arr_cond,True),True,cond_match_lag)) as cond_match_lag
    from final_t_1
   where cond_match1 = True
)

select *, 
       sum(price_diff) over(partition by exchange order by datetime, market rows between unbounded PRECEDING and CURRENT row) as price_diff_market
from (
    select * except(arr_cond) from final_t
    where (not cond_match_lag)
)
order by datetime desc;
