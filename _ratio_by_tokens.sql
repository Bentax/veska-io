WITH exchange_data_frame AS (
    SELECT
        toDateTime(agg_timestamp / 1000) AS datetime,
        exchange,
        market,
        groupArray(volume_base_buy_taker)[1] AS vbbt,
        groupArray(volume_base_sell_taker)[1] AS vbst
    FROM aggregates_1h
    WHERE
        market IN (${market})
        AND exchange = '${exchange}'
        AND toDateTime(agg_timestamp / 1000) >= date_sub(hour, 2 * ${window_size}, $__fromTime)
        AND toDateTime(agg_timestamp / 1000) <= $__toTime
    GROUP BY datetime, exchange, market
),

exchange_data_frame_by_interval AS (
    SELECT
        exchange,
        market,
        datetime AS grouping_datetime,
        sum((vbbt - vbst) / greatest(vbbt, vbst)) AS __volume_base
    FROM exchange_data_frame
    GROUP BY grouping_datetime, exchange, market
)

SELECT
    grouping_datetime AS time,
    market,
    anyLast(__volume_base) OVER (
        PARTITION BY market, exchange
        ORDER BY grouping_datetime ASC
        RANGE BETWEEN toUInt64((${window_size} - 1) * 60 * 60) PRECEDING AND CURRENT ROW
    ) AS "_"
FROM exchange_data_frame_by_interval
ORDER BY time;
