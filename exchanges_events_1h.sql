CREATE TABLE exchanges_events_1h
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

message Event {
	string event = 1;
	uint64 event_timestamp = 2;
	
	string exchange = 3;
	string market = 4;
	string base = 5;
	string quot = 6;

	optional double price_open = 7;
	optional double price_close = 8;
	optional double price_high = 9;
	optional double price_low = 10;

	optional double volume_quot = 11;
	optional double volume_base = 12;
	optional double volume_base_sell_taker = 13;
	optional double volume_base_buy_taker = 14;

	optional double oi_open = 15;

	optional int32 trades_count = 16;

	optional int32 liquidations_sell_count = 17;
	optional int32 liquidations_buy_count = 18;
	optional double liquidations_sell_base_volume = 19;
	optional double liquidations_buy_base_volume = 20;
	optional double liquidations_sell_quot_volume = 21;
	optional double liquidations_buy_quot_volume = 22;
}
