SELECT
  date,
  total_fees,
  pool_revenue,
  total_value_locked,
  daily_apr,
  weekly_moving_avg,
  monthly_moving_avg,
  quarterly_moving_avg,
  half_year_moving_avg,
  yearly_moving_avg
FROM dune.guamacole.result_jlp_daily_apr
ORDER BY date DESC