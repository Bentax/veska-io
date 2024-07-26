--EXCEPT--
#
#
SELECT count(*)
FROM binance_futures_klines_1h
--WHERE kline_timestamp >= (toUnixTimestamp64Milli(now64(3)) - 3600000)
GROUP BY updated_timestamp, kline_timestamp, base, quot
HAVING count(*) > 1
-- result NULL
######
SELECT count(*)
FROM binance_futures_klines_1h
--WHERE kline_timestamp >= (toUnixTimestamp64Milli(now64(3)) - 3600000)
GROUP BY updated_timestamp, base, quot
HAVING count(*) > 1
-- result NOT NULL
######
