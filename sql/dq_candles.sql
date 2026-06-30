SELECT
    (SELECT count(*) FROM candles_1m
       WHERE NOT (high >= low AND high >= open AND high >= close
                  AND low <= open AND low <= close
                  AND low > 0 AND volume >= 0 AND trade_count > 0))
  + (SELECT count(*) FROM candles_1h
       WHERE NOT (high >= low AND high >= open AND high >= close
                  AND low <= open AND low <= close
                  AND low > 0 AND volume >= 0 AND trade_count > 0))
    AS bad_rows;