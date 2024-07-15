DESCRIBE binance_futures_klines_1h

symbol	String					
close	String					
volume	String					
trade_num	UInt64					
quot_asset_volume	String					
taker_buy_base_asset_volume	String					
taker_buy_quot_asset_volume	String					
updated_timestamp	UInt64	DEFAULT	toUnixTimestamp64Milli(now64(3))			
base	String					
quot	String					
kline_timestamp	UInt64					
open_time	UInt64					
close_time	UInt64					
open	String					
high	String					
low	String					
