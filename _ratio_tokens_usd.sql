WITH exchange_data_frame_B AS (
    SELECT
        toDateTime(agg_timestamp/1000) AS datetime,
        exchange, 
        market,
        groupArray(volume_base_buy_taker)[1] AS vbbt,
        groupArray(volume_base_sell_taker)[1] AS vbst
    FROM aggregates_1h
    WHERE
        market IN (${market})
        AND exchange = '${exchange}'
        AND toDateTime(agg_timestamp/1000) >= date_sub(hour, 2*${window_size}, $__fromTime)
        AND toDateTime(agg_timestamp/1000) <= $__toTime 
    GROUP BY agg_timestamp, exchange, market
),

exchange_data_frame_Q AS (
    SELECT
        toDateTime(agg_timestamp/1000) AS datetime,
        exchange, 
        market,
        groupArray(volume_quot_buy_taker)[1] AS vqbt,
        groupArray(volume_quot_sell_taker)[1] AS vqst
    FROM aggregates_1h
    WHERE
        market IN (${market})
        AND exchange = '${exchange}'
        AND toDateTime(agg_timestamp/1000) >= date_sub(hour, 2*${window_size}, $__fromTime)
        AND toDateTime(agg_timestamp/1000) <= $__toTime 
    GROUP BY agg_timestamp, exchange, market
),

exchange_data_frame_by_interval_B AS (
    SELECT
        exchange,
        market,
        sum((vbbt - vbst) / greatest(vbbt, vbst)) AS __volume_base,
        datetime AS grouping_datetime
    FROM exchange_data_frame_B
    GROUP BY grouping_datetime, exchange, market
),

exchange_data_frame_by_interval_Q AS (
    SELECT
        exchange,
        market,
        sum((vqbt - vqst) / greatest(vqbt, vqst)) AS __volume_quot,
        datetime AS grouping_datetime
    FROM exchange_data_frame_Q
    GROUP BY grouping_datetime, exchange, market
),

final_data_frame AS (
    SELECT
        B.grouping_datetime AS time,
        B.market,
        abs(anyLast(B.__volume_base) OVER (
            PARTITION BY B.market, B.exchange ORDER BY B.grouping_datetime ASC 
            RANGE BETWEEN toUInt64((${window_size}-1)*60*60) PRECEDING AND CURRENT ROW)) 
        - abs(anyLast(Q.__volume_quot) OVER (
            PARTITION BY Q.market, Q.exchange ORDER BY Q.grouping_datetime ASC 
            RANGE BETWEEN toUInt64((${window_size}-1)*60*60) PRECEDING AND CURRENT ROW)) AS result
    FROM exchange_data_frame_by_interval_B B
    JOIN exchange_data_frame_by_interval_Q Q 
    ON B.grouping_datetime = Q.grouping_datetime AND B.market = Q.market AND B.exchange = Q.exchange
)

SELECT
    time,
    market,
    result AS "_"
FROM final_data_frame
ORDER BY time ASC;
