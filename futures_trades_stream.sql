CREATE TABLE IF NOT EXISTS futures_trades_stream
(
updated_at DateTime DEFAULT now(),
trade_timestamp UInt64 NOT NULL,
created_at_height Nullable(UInt64) DEFAULT NULL,

trade_id Nullable(String) DEFAULT NULL,
agg_id Nullable(String) DEFAULT NULL,
start_trade_id Nullable(String) DEFAULT NULL,
end_trade_id Nullable(String) DEFAULT NULL,

exchange String NOT NULL,
market String NOT NULL,
base String NOT NULL,
quot String NOT NULL,
side String NOT NULL,
size Float64 NOT NULL,
price Float64 NOT NULL,
trade_type Nullable(String) DEFAULT NULL,
is_buyer_maker Nullable(bool) DEFAULT NULL
) ENGINE = MergeTree
PARTITION BY toYear(fromUnixTimestamp(trade_timestamp))
ORDER BY (trade_timestamp, exchange, market)
