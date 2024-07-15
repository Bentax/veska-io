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
