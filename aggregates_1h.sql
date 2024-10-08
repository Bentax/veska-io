CREATE TABLE IF NOT EXISTS aggregates_1h
(
    agg_timestamp UInt64,
    exchange String,
    market String,
    base String,
    quot String,
    price_open Nullable(Float64),
    price_close Nullable(Float64),
    price_high Nullable(Float64),
    price_low Nullable(Float64),
    volume_base Nullable(Float64),
    volume_quot Nullable(Float64),
    volume_base_buy_taker Nullable(Float64),
    volume_quot_buy_taker Nullable(Float64),
    volume_base_sell_taker Nullable(Float64),
    volume_quot_sell_taker Nullable(Float64),
    oi_open Nullable(Float64),
    trades_count Nullable(Int32),
    liquidations_shorts_count Nullable(Int32),
    liquidations_longs_count Nullable(Int32),
    liquidations_shorts_base_volume	Nullable(Float64),
    liquidations_longs_base_volume	Nullable(Float64),
    liquidations_shorts_quot_volume	Nullable(Float64),
    liquidations_longs_quot_volume Nullable(Float64),
    funding_rate Nullable(Float64),
    updated_timestamp UInt64 DEFAULT toUnixTimestamp64Milli(now64(3))
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYear(fromUnixTimestamp(agg_timestamp))
ORDER BY (agg_timestamp, exchange, market);
--
INSERT INTO aggregates_1h
SELECT
    event_timestamp AS agg_timestamp,
    exchange,
    market,
    base,
    quot,
    any(price_open) AS price_open,
    any(price_close) AS price_close,
    any(price_high) AS price_high,
    any(price_low) AS price_low,
    any(volume_quot) AS volume_quot,
    any(volume_base) AS volume_base,
    any(volume_base_sell_taker) AS volume_base_sell_taker,
    any(volume_base_buy_taker) AS volume_base_buy_taker,
    any(oi_open) AS oi_open,
    any(trades_count) AS trades_count,
    any(liquidations_sell_count) AS liquidations_sell_count,
    any(liquidations_buy_count) AS liquidations_buy_count,
    any(liquidations_sell_base_volume) AS liquidations_sell_base_volume,
    any(liquidations_buy_base_volume) AS liquidations_buy_base_volume,
    any(liquidations_sell_quot_volume) AS liquidations_sell_quot_volume,
    any(liquidations_buy_quot_volume) AS liquidations_buy_quot_volume,
    toUnixTimestamp64Milli(now64(3)) AS updated_timestamp
FROM (
    SELECT *,
        row_number() OVER (PARTITION BY event_timestamp,market,event ORDER BY updated_timestamp DESC) AS rn
    FROM exchanges_events_1h
)
WHERE rn = 1
GROUP BY agg_timestamp, exchange, market, base, quot
ORDER BY updated_timestamp DESC;


SELECT
    event_timestamp AS agg_timestamp,
    exchange,
    market,
    base,
    quot,
    any(price_open) AS price_open,
    any(price_close) AS price_close,
    any(price_high) AS price_high,
    any(price_low) AS price_low,
    any(volume_base) AS volume_base,
    any(volume_quot) AS volume_quot,
    any(volume_base_buy_taker) AS volume_base_buy_taker,
    any(volume_quot_buy_taker) AS volume_quot_buy_taker,
    any(volume_base_sell_taker) AS volume_base_sell_taker,
    any(volume_quot_sell_taker) AS volume_quot_sell_taker,
    any(oi_open) AS oi_open,
    any(trades_count) AS trades_count,
    any(liquidations_shorts_count) AS liquidations_shorts_count,
    any(liquidations_longs_count) AS liquidations_longs_count,
    any(liquidations_shorts_base_volume) AS liquidations_shorts_base_volume,
    any(liquidations_longs_base_volume) AS liquidations_longs_base_volume,
    any(liquidations_shorts_quot_volume) AS liquidations_shorts_quot_volume,
    any(liquidations_longs_quot_volume) AS liquidations_longs_quot_volume,
    any(funding_rate) AS funding_rate,
    toUnixTimestamp64Milli(now64(3)) AS updated_timestamp
FROM (
    SELECT *,
        row_number() OVER (PARTITION BY event_timestamp,market,event ORDER BY updated_timestamp DESC) AS rn
    FROM futures_exchanges_events_1h
)
WHERE rn = 1
GROUP BY agg_timestamp, exchange, market, base, quot
ORDER BY updated_timestamp DESC;
