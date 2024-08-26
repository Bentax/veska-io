WITH pre_load AS (
    SELECT
        toDateTime(agg_timestamp / 1000) AS grouping_datetime,
        exchange, 
        market,
        toDecimal32(anyLast(IF(funding_rate != 0, funding_rate * 100, NULL)), 8) AS rate,
        anyLast(IF(price_open != 0, price_open, NULL)) AS price,
        MIN(IF(price_low != 0, price_low, NULL)) AS min_price,
        MAX(IF(price_high != 0, price_high, NULL)) AS max_price
    FROM public.aggregates_1h
    WHERE
        exchange = 'binance'
    GROUP BY grouping_datetime, exchange, market
),
max_date AS (
    SELECT 
        MAX(toDateTime(agg_timestamp / 1000)) AS m_date,
        IF(
            date_trunc('hour', toDateTime(now())) > MAX(toDateTime(agg_timestamp / 1000)), 
            MAX(toDateTime(agg_timestamp / 1000)),
            toStartOfInterval(date_trunc('hour', toDateTime(now())), interval IF(exchange = 'dydx', 1, 8) hour)
        ) AS to_filtDate,
        MAX(IF(price_open != 0, toDateTime(agg_timestamp / 1000), NULL)) AS max_price_dt_1,
        IF(
            date_trunc('hour', toDateTime(now())) > MAX(IF(price_open != 0, toDateTime(agg_timestamp / 1000), NULL)), 
            MAX(IF(price_open != 0, toDateTime(agg_timestamp / 1000), NULL)),
            toStartOfInterval(date_trunc('hour', toDateTime(now())), interval IF(exchange = 'dydx', 1, 8) hour)
        ) AS max_price_dt
    FROM public.aggregates_1h 
    WHERE exchange = 'binance'
),
stats2 AS (
    SELECT  
        market AS m2, 
        MAX(price_open) AS now_price
    FROM public.aggregates_1h
    WHERE 
        price_open != 0 
        AND exchange = 'binance' 
        AND toDateTime(agg_timestamp / 1000) = (SELECT max_price_dt FROM max_date)
    GROUP BY market
)
SELECT 
    p.market, 
    p.grouping_datetime AS datetime, 
    p.rate, 
    p.price AS price1,
    ((s2.now_price - p.price) / p.price) * 100 AS price_change,
    s2.now_price,
    s2.m2 AS market2
FROM pre_load p
LEFT JOIN stats2 s2 ON s2.m2 = p.market
WHERE p.grouping_datetime = (SELECT to_filtDate FROM max_date);
