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
    updated_timestamp UInt64 DEFAULT toUnixTimestamp64Milli(now())
) 
ENGINE = MergeTree()
ORDER BY event_timestamp
TTL event_timestamp + INTERVAL 40 DAY;

-- Агрегация для события 'price'
INSERT INTO exchanges_events_1h
SELECT
    'price' AS event,
    toUnixTimestamp(toStartOfHour(event_time)) AS event_timestamp,
    exchange,
    market,
    base,
    quot,
    MIN(price) AS price_low,
    MAX(price) AS price_high,
    FIRST_VALUE(price) AS price_open,
    LAST_VALUE(price) AS price_close,
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
    toUnixTimestamp64Milli(now()) AS updated_timestamp
FROM futures_trades_stream
GROUP BY
    exchange,
    market,
    base,
    quot,
    toStartOfHour(event_time);

-- Агрегация для события 'volume'
INSERT INTO exchanges_events_1h
SELECT
    'volume' AS event,
    toUnixTimestamp(toStartOfHour(event_time)) AS event_timestamp,
    exchange,
    market,
    base,
    quot,
    NULL AS price_low,
    NULL AS price_high,
    NULL AS price_open,
    NULL AS price_close,
    SUM(volume_quot) AS volume_quot,
    SUM(volume_base) AS volume_base,
    SUM(volume_base_sell_taker) AS volume_base_sell_taker,
    SUM(volume_base_buy_taker) AS volume_base_buy_taker,
    NULL AS oi_open,
    NULL AS trades_count,
    NULL AS liquidations_sell_count,
    NULL AS liquidations_buy_count,
    NULL AS liquidations_sell_base_volume,
    NULL AS liquidations_buy_base_volume,
    NULL AS liquidations_sell_quot_volume,
    NULL AS liquidations_buy_quot_volume,
    toUnixTimestamp64Milli(now()) AS updated_timestamp
FROM futures_trades_stream
GROUP BY
    exchange,
    market,
    base,
    quot,
    toStartOfHour(event_time);

-- Агрегация для события 'trades'
INSERT INTO exchanges_events_1h
SELECT
    'trades' AS event,
    toUnixTimestamp(toStartOfHour(event_time)) AS event_timestamp,
    exchange,
    market,
    base,
    quot,
    NULL AS price_low,
    NULL AS price_high,
    NULL AS price_open,
    NULL AS price_close,
    NULL AS volume_quot,
    NULL AS volume_base,
    NULL AS volume_base_sell_taker,
    NULL AS volume_base_buy_taker,
    NULL AS oi_open,
    COUNT(*) AS trades_count,
    NULL AS liquidations_sell_count,
    NULL AS liquidations_buy_count,
    NULL AS liquidations_sell_base_volume,
    NULL AS liquidations_buy_base_volume,
    NULL AS liquidations_sell_quot_volume,
    NULL AS liquidations_buy_quot_volume,
    toUnixTimestamp64Milli(now()) AS updated_timestamp
FROM futures_trades_stream
GROUP BY
    exchange,
    market,
    base,
    quot,
    toStartOfHour(event_time);

-- Агрегация для события 'liquidations'
INSERT INTO exchanges_events_1h
SELECT
    'liquidations' AS event,
    toUnixTimestamp(toStartOfHour(event_time)) AS event_timestamp,
    exchange,
    market,
    base,
    quot,
    NULL AS price_low,
    NULL AS price_high,
    NULL AS price_open,
    NULL AS price_close,
    NULL AS volume_quot,
    NULL AS volume_base,
    NULL AS volume_base_sell_taker,
    NULL AS volume_base_buy_taker,
    NULL AS oi_open,
    NULL AS trades_count,
    SUM(liquidations_sell_count) AS liquidations_sell_count,
    SUM(liquidations_buy_count) AS liquidations_buy_count,
    SUM(liquidations_sell_base_volume) AS liquidations_sell_base_volume,
    SUM(liquidations_buy_base_volume) AS liquidations_buy_base_volume,
    SUM(liquidations_sell_quot_volume) AS liquidations_sell_quot_volume,
    SUM(liquidations_buy_quot_volume) AS liquidations_buy_quot_volume,
    toUnixTimestamp64Milli(now()) AS updated_timestamp
FROM futures_trades_stream
GROUP BY
    exchange,
    market,
    base,
    quot,
    toStartOfHour(event_time);
