event	String
event_timestamp	UInt64
exchange	String
market	String
base	String
quot	String
price_open	Nullable(Float64)
price_close	Nullable(Float64)
price_high	Nullable(Float64)
price_low	Nullable(Float64)
volume_base	Nullable(Float64)
volume_quot	Nullable(Float64)
volume_base_buy_taker	Nullable(Float64)
volume_quot_buy_taker	Nullable(Float64)
volume_base_sell_taker	Nullable(Float64)
volume_quot_sell_taker	Nullable(Float64)
oi_open	Nullable(Float64)
trades_count	Nullable(UInt32)
liquidations_shorts_count	Nullable(UInt32)
liquidations_longs_count	Nullable(UInt32)
liquidations_shorts_base_volume	Nullable(Float64)
liquidations_longs_base_volume	Nullable(Float64)
liquidations_shorts_quot_volume	Nullable(Float64)
liquidations_longs_quot_volume	Nullable(Float64)
funding_rate	Nullable(Float64)
updated_timestamp	UInt64
############ DESCRIBE futures_exchanges_events_1h
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
    updated_timestamp UInt64 DEFAULT toUnixTimestamp64Milli(now64(3))
) 
ENGINE = MergeTree()
ORDER BY (event_timestamp, exchange, market)
TTL toDate(updated_timestamp/1000) + INTERVAL 40 DAY;

SELECT
    'price' AS event,
    kline_timestamp AS event_timestamp,
    'binance' AS exchange,
    concat(lower(base),'-usd') AS market,
    lower(base) AS base,
    'usd' AS quot,
    anyLast(toFloat64(open)) AS price_open,
    anyLast(toFloat64(close)) AS price_close,
    anyLast(toFloat64(high)) AS price_high,
    anyLast(toFloat64(low)) AS price_low,
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
    NULL AS funding_rate,
    toUnixTimestamp64Milli(now64(3)) AS updated_timestamp
FROM binance_futures_klines_1h
WHERE kline_timestamp >= (toUnixTimestamp64Milli(now64(3)) - 3600000)
GROUP BY kline_timestamp, exchange, market, base, quot
ORDER BY updated_timestamp DESC

UNION ALL

SELECT
    'volume' AS event,
    kline_timestamp AS event_timestamp,
    'binance' AS exchange,
    concat(lower(base),'-usd') AS market,
    lower(base) AS base,
    'usd' AS quot,
    NULL AS price_open,
    NULL AS price_close,
    NULL AS price_high,
    NULL AS price_low,
    anyLast(toUInt64OrNull(volume)) AS volume_base,
    anyLast(toFloat64(quot_asset_volume)) AS volume_quot,
    anyLast(toUInt64OrNull(taker_buy_base_asset_volume)) AS volume_base_buy_taker,
    anyLast(toFloat64(taker_buy_quot_asset_volume)) AS volume_quot_buy_taker,
    anyLast(toFloat64(volume) - toFloat64(taker_buy_base_asset_volume)) AS volume_base_sell_taker,
    anyLast(toFloat64(quot_asset_volume) - toFloat64(taker_buy_quot_asset_volume)) AS volume_quot_sell_taker,
    NULL AS oi_open,
    NULL AS trades_count,
    NULL AS liquidations_shorts_count,
    NULL AS liquidations_longs_count,
    NULL AS liquidations_shorts_base_volume,
    NULL AS liquidations_longs_base_volume,
    NULL AS liquidations_shorts_quot_volume,
    NULL AS liquidations_longs_quot_volume,
    NULL AS funding_rate,
    toUnixTimestamp64Milli(now64(3)) AS updated_timestamp
FROM binance_futures_klines_1h
WHERE kline_timestamp >= (toUnixTimestamp64Milli(now64(3)) - 3600000)
GROUP BY kline_timestamp, exchange, market, base, quot
ORDER BY updated_timestamp DESC

UNION ALL

SELECT
    'trades' AS event,
    kline_timestamp AS event_timestamp,
    'binance' AS exchange,
    concat(lower(base),'-usd') AS market,
    lower(base) AS base,
    'usd' AS quot,
    NULL AS price_open,
    NULL AS price_close,
    NULL AS price_high,
    NULL AS price_low,
    NULL AS volume_quot,
    NULL AS volume_base,
    NULL AS volume_base_buy_taker,
    NULL AS volume_quot_buy_taker,
    NULL AS volume_base_sell_taker,
    NULL AS volume_quot_sell_taker,
    NULL AS oi_open,
    anyLast(trade_num) AS trades_count,
    NULL AS liquidations_shorts_count,
    NULL AS liquidations_longs_count,
    NULL AS liquidations_shorts_base_volume,
    NULL AS liquidations_longs_base_volume,
    NULL AS liquidations_shorts_quot_volume,
    NULL AS liquidations_longs_quot_volume,
    NULL AS funding_rate,
    toUnixTimestamp64Milli(now64(3)) AS updated_timestamp
FROM binance_futures_klines_1h
WHERE kline_timestamp >= (toUnixTimestamp64Milli(now64(3)) - 3600000)
GROUP BY kline_timestamp, exchange, market, base, quot
ORDER BY updated_timestamp DESC

UNION ALL

SELECT
    'funding' AS event,
    funding_timestamp AS event_timestamp,
    'binance' AS exchange,
    concat(lower(base),'-usd') AS market,
    lower(base) AS base,
    'usd' AS quot,
    NULL AS price_open,
    NULL AS price_close,
    NULL AS price_high,
    NULL AS price_low,
    NULL AS volume_quot,
    NULL AS volume_base,
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
    anyLast(toFloat64(rate)) AS funding_rate,
    toUnixTimestamp64Milli(now64(3)) AS updated_timestamp
FROM binance_futures_funding_1h
WHERE funding_timestamp >= (toUnixTimestamp64Milli(now64(3)) - 3600000)
GROUP BY funding_timestamp, exchange, market, base, quot
ORDER BY updated_timestamp DESC
