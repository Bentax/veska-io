CREATE TABLE IF NOT EXISTS futures_exchanges_events_1h
(
    event String,
    event_timestamp UInt64,
    exchange String,
    market String,
    base String,
    quot String,
    price_open Nullable(Float64),
    price_close Nullable(Float64),
    price_high Nullable(Float64),
    price_low Nullable(Float64),
    volume_quot Nullable(Float64),
    volume_base Nullable(Float64),
    volume_base_sell_taker Nullable(Float64),
    volume_base_buy_taker Nullable(Float64),
    oi_open Nullable(Float64),
    trades_count Nullable(Int32),
    liquidations_sell_count Nullable(Int32),
    liquidations_buy_count Nullable(Int32),
    liquidations_sell_base_volume Nullable(Float64),
    liquidations_buy_base_volume Nullable(Float64),
    liquidations_sell_quot_volume Nullable(Float64),
    liquidations_buy_quot_volume Nullable(Float64),
    updated_timestamp UInt64 DEFAULT toUnixTimestamp64Milli(now64(3))
) 
ENGINE = MergeTree()
ORDER BY (event_timestamp, exchange, market)
TTL toDate(updated_timestamp/1000) + INTERVAL 40 DAY;

-- ЗАПРОС

INSERT INTO exchanges_events_1h
SELECT *
FROM (
    SELECT
        'price' AS event,
        trade_timestamp AS event_timestamp,
        exchange,
        market,
        base,
        quot,
        any(price) AS price_open,
        anyLast(price) AS price_close,
        max(price) AS price_high,
        min(price) AS price_low,
        NULL AS volume_base,
        NULL AS volume_quot,
        NULL AS volume_base_buy_taker,
        NULL AS volume_quot_buy_taker,
        NULL AS volume_base_sell_taker,
        NULL AS volume_quot_sell_taker,
        NULL AS oi_open,
        NULL AS trades_count,
        NULL AS liquidations_shorts_count,
        NULL AS liquidations_longs_count,
        NULL AS liquidations_shorts_base_volume,
        NULL AS liquidations_longs_base_volume,
        NULL AS liquidations_shorts_quot_volume,
        NULL AS liquidations_longs_quot_volume,
        toUnixTimestamp64Milli(now64(3)) AS updated_timestamp
    FROM futures_trades_stream
    WHERE trade_timestamp >= (toUnixTimestamp(toStartOfHour(now())) - 3600) * 1000
    GROUP BY event_timestamp, exchange, market, base, quot
    ORDER BY event, event_timestamp desc

    UNION ALL

    SELECT
        'volume' AS event,
        trade_timestamp AS event_timestamp,
        exchange,
        market,
        base,
        quot,
        NULL AS price_open,
        NULL AS price_close,
        NULL AS price_high,
        NULL AS price_low,
        sum(price * size) AS volume_quot,
        sum(size) AS volume_base,
        sumIf(size, side = 'BUY' AND trade_type = 'LIMIT') AS volume_base_sell_taker,
        sumIf(size, side = 'SELL' AND trade_type = 'LIMIT') AS volume_base_buy_taker,
        NULL AS oi_open,
        NULL AS trades_count,
        NULL AS liquidations_sell_count,
        NULL AS liquidations_buy_count,
        NULL AS liquidations_sell_base_volume,
        NULL AS liquidations_buy_base_volume,
        NULL AS liquidations_sell_quot_volume,
        NULL AS liquidations_buy_quot_volume,
        toUnixTimestamp64Milli(now64(3)) AS updated_timestamp
    FROM futures_trades_stream
    WHERE trade_timestamp >= (toUnixTimestamp(toStartOfHour(now())) - 3600) * 1000
    GROUP BY event_timestamp, exchange, market, base, quot
    ORDER BY event, event_timestamp desc

    UNION ALL

    SELECT
        'trades' AS event,
        trade_timestamp AS event_timestamp,
        exchange,
        market,
        base,
        quot,
        NULL AS price_open,
        NULL AS price_close,
        NULL AS price_high,
        NULL AS price_low,
        NULL AS volume_quot,
        NULL AS volume_base,
        NULL AS volume_base_sell_taker,
        NULL AS volume_base_buy_taker,
        NULL AS oi_open,
        count() AS trades_count,
        NULL AS liquidations_sell_count,
        NULL AS liquidations_buy_count,
        NULL AS liquidations_sell_base_volume,
        NULL AS liquidations_buy_base_volume,
        NULL AS liquidations_sell_quot_volume,
        NULL AS liquidations_buy_quot_volume,
        toUnixTimestamp64Milli(now64(3)) AS updated_timestamp
    FROM futures_trades_stream
    WHERE trade_timestamp >= (toUnixTimestamp(toStartOfHour(now())) - 3600) * 1000
    GROUP BY event_timestamp, exchange, market, base, quot
    ORDER BY event, event_timestamp desc

    UNION ALL

    SELECT
        'liquidations' AS event,
        trade_timestamp AS event_timestamp,
        exchange,
        market,
        base,
        quot,
        NULL AS price_open,
        NULL AS price_close,
        NULL AS price_high,
        NULL AS price_low,
        NULL AS volume_quot,
        NULL AS volume_base,
        NULL AS volume_base_sell_taker,
        NULL AS volume_base_buy_taker,
        NULL AS oi_open,
        NULL AS trades_count,
        countIf(side = 'SELL' AND trade_type = 'LIQUIDATED') AS liquidations_sell_count,
        countIf(side = 'BUY' AND trade_type = 'LIQUIDATED') AS liquidations_buy_count,
        sumIf(size, side = 'SELL' AND trade_type = 'LIQUIDATED') AS liquidations_sell_base_volume,
        sumIf(size, side = 'BUY' AND trade_type = 'LIQUIDATED') AS liquidations_buy_base_volume,
        sumIf(price * size, side = 'SELL' AND trade_type = 'LIQUIDATED') AS liquidations_sell_quot_volume,
        sumIf(price * size, side = 'BUY' AND trade_type = 'LIQUIDATED') AS liquidations_buy_quot_volume,
        toUnixTimestamp64Milli(now64(3)) AS updated_timestamp
    FROM futures_trades_stream
    WHERE trade_timestamp >= (toUnixTimestamp(toStartOfHour(now())) - 3600) * 1000
    GROUP BY event_timestamp, exchange, market, base, quot
    ORDER BY event, event_timestamp desc
);
