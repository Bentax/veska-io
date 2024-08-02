SELECT
  tokens_ratio.grouped_datetime as "Time",
  tokens_ratio.market,
  ABS(tokens_ratio.B) - ABS(usd_ratio.Q) as "Diff"
FROM (
  SELECT
    grouped_datetime,
    market,
    AVG(B) as B
  FROM (
    SELECT
      toStartOfInterval(toDateTime(agg_timestamp/1000), INTERVAL ${granularity}) as grouped_datetime,
      market,
      (volume_base_buy_taker - volume_base_sell_taker) / NULLIF(greatest(volume_base_buy_taker, volume_base_sell_taker), 0) as B
    FROM "aggregates_1h"
    WHERE 
      exchange = '${exchange}'
      AND toDateTime(agg_timestamp/1000) >= $__fromTime 
      AND toDateTime(agg_timestamp/1000) <= $__toTime 
  )
  GROUP BY grouped_datetime, market
) as tokens_ratio
JOIN (
  SELECT
    grouped_datetime,
    market,
    AVG(Q) as Q
  FROM (
    SELECT
      toStartOfInterval(toDateTime(agg_timestamp/1000), INTERVAL ${granularity}) as grouped_datetime,
      market,
      (volume_quot_buy_taker - volume_quot_sell_taker) / NULLIF(greatest(volume_quot_buy_taker, volume_quot_sell_taker), 0) as Q
    FROM "aggregates_1h"
    WHERE 
      exchange = '${exchange}'
      AND toDateTime(agg_timestamp/1000) >= $__fromTime 
      AND toDateTime(agg_timestamp/1000) <= $__toTime 
  )
  GROUP BY grouped_datetime, market
) as usd_ratio
ON tokens_ratio.grouped_datetime = usd_ratio.grouped_datetime
AND tokens_ratio.market = usd_ratio.market
ORDER BY tokens_ratio.grouped_datetime ASC;
