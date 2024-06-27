syntax = "proto3";

package exchanges_events;

option go_package = "github.com/veska-io/streams-proto/streams;eeventspb";

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
