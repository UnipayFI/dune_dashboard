WITH last_30d_avg AS (
  SELECT AVG(daily_apr) as avg_daily_apr
  FROM dune.guamacole.result_jlp_daily_apr
  WHERE date >= NOW() - INTERVAL '30' DAY
)
SELECT 
  ROUND(avg_daily_apr * 100 * 0.7, 4) as expected_pure_jlp_arb_apr
FROM last_30d_avg 