CREATE TABLE IF NOT EXISTS exchanges_events_1h
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
    updated_timestamp UInt64 DEFAULT toUnixTimestamp64Milli(cast(now(),'DateTime64'))
) 
ENGINE = MergeTree()
ORDER BY event_timestamp
TTL toDateTime(updated_timestamp) + INTERVAL 40 DAY;

-- ЗАПРОС

INSERT INTO exchanges_events_1h
SELECT
    event,
    event_timestamp,
    exchange,
    market,
    base,
    quot,
    price_open,
    price_close,
    price_high,
    price_low,
    volume_quot,
    volume_base,
    volume_base_sell_taker,
    volume_base_buy_taker,
    oi_open,
    trades_count,
    liquidations_sell_count,
    liquidations_buy_count,
    liquidations_sell_base_volume,
    liquidations_buy_base_volume,
    liquidations_sell_quot_volume,
    liquidations_buy_quot_volume,
    updated_timestamp
FROM
(
    SELECT
        'price' AS event,
        toUnixTimestamp(toStartOfHour(toDateTime(trade_timestamp))) AS event_timestamp,
        exchange,
        market,
        base,
        quot,
        any(price) AS price_open,
        anyLast(price) AS price_close,
        max(price) AS price_high,
        min(price) AS price_low,
        NULL AS volume_quot,
        NULL AS volume_base,
        NULL AS volume_base_sell_taker,
        NULL AS volume_base_buy_taker,
        NULL AS oi_open,
        NULL AS trades_count,
        NULL AS liquidations_sell_count,
        NULL AS liquidations_buy_count,
        NULL AS liquidations_sell_base_volume,
        NULL AS liquidations_buy_base_volume,
        NULL AS liquidations_sell_quot_volume,
        NULL AS liquidations_buy_quot_volume,
        toUnixTimestamp(now()) * 1000 AS updated_timestamp
    FROM futures_trades_stream
    GROUP BY event_timestamp, exchange, market, base, quot

    UNION ALL

    SELECT
        'volume' AS event,
        toUnixTimestamp(toStartOfHour(toDateTime(trade_timestamp))) AS event_timestamp,
        exchange,
        market,
        base,
        quot,
        NULL AS price_open,
        NULL AS price_close,
        NULL AS price_high,
        NULL AS price_low,
        sum(size * price) AS volume_quot,
        sum(size) AS volume_base,
        sumIf(size, side = 'sell') AS volume_base_sell_taker,
        sumIf(size, side = 'buy') AS volume_base_buy_taker,
        NULL AS oi_open,
        NULL AS trades_count,
        NULL AS liquidations_sell_count,
        NULL AS liquidations_buy_count,
        NULL AS liquidations_sell_base_volume,
        NULL AS liquidations_buy_base_volume,
        NULL AS liquidations_sell_quot_volume,
        NULL AS liquidations_buy_quot_volume,
        toUnixTimestamp(now()) * 1000 AS updated_timestamp
    FROM futures_trades_stream
    GROUP BY event_timestamp, exchange, market, base, quot

    UNION ALL

    SELECT
        'trades' AS event,
        toUnixTimestamp(toStartOfHour(toDateTime(trade_timestamp))) AS event_timestamp,
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
        toUnixTimestamp(now()) * 1000 AS updated_timestamp
    FROM futures_trades_stream
    GROUP BY event_timestamp, exchange, market, base, quot

    UNION ALL

    SELECT
        'liquidations' AS event,
        toUnixTimestamp(toStartOfHour(toDateTime(trade_timestamp))) AS event_timestamp,
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
        countIf(trade_type = 'liquidation' AND side = 'sell') AS liquidations_sell_count,
        countIf(trade_type = 'liquidation' AND side = 'buy') AS liquidations_buy_count,
        sumIf(size, trade_type = 'liquidation' AND side = 'sell') AS liquidations_sell_base_volume,
        sumIf(size, trade_type = 'liquidation' AND side = 'buy') AS liquidations_buy_base_volume,
        sumIf(size * price, trade_type = 'liquidation' AND side = 'sell') AS liquidations_sell_quot_volume,
        sumIf(size * price, trade_type = 'liquidation' AND side = 'buy') AS liquidations_buy_quot_volume,
        toUnixTimestamp(now()) * 1000 AS updated_timestamp
    FROM futures_trades_stream
    GROUP BY event_timestamp, exchange, market, base, quot
)
