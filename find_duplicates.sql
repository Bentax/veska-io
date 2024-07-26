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
SELECT sum(qwerty) FROM
(SELECT count() AS qwerty
    FROM binance_futures_klines_1h
    GROUP BY updated_timestamp, base, quot
    HAVING count() > 1)
-- result 7174218
######
SELECT avg(qwerty) FROM
(SELECT count() AS qwerty
    FROM binance_futures_klines_1h
    GROUP BY updated_timestamp, base, quot
    HAVING count() > 1)
-- result 196.41400646115096
