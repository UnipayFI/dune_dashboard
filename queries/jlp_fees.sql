WITH pools AS (
  WITH base AS (
    SELECT
      'deposit' AS position_change,
      TO_BASE58(BYTEARRAY_SUBSTRING(data, 1 + 16, 32)) AS custody_key,
      BYTEARRAY_TO_BIGINT(BYTEARRAY_REVERSE(BYTEARRAY_SUBSTRING(data, 1 + 80, 8))) AS amount_in_pre,
      BYTEARRAY_TO_BIGINT(BYTEARRAY_REVERSE(BYTEARRAY_SUBSTRING(data, 1 + 120, 8))) AS amount_in_post,
      BYTEARRAY_TO_BIGINT(BYTEARRAY_REVERSE(BYTEARRAY_SUBSTRING(data, 1 + 104, 8))) / 1e6 AS amount_in_pre_usd,
      BYTEARRAY_TO_BIGINT(BYTEARRAY_REVERSE(BYTEARRAY_SUBSTRING(data, 1 + 128, 8))) / 1e6 AS amount_in_post_usd,
      BYTEARRAY_TO_BIGINT(BYTEARRAY_REVERSE(BYTEARRAY_SUBSTRING(data, 1 + 104, 8))) / 1e6 - BYTEARRAY_TO_BIGINT(BYTEARRAY_REVERSE(BYTEARRAY_SUBSTRING(data, 1 + 128, 8))) / 1e6 AS fee_usd,
      block_slot,
      block_time,
      tx_id
    FROM solana.instruction_calls
    WHERE
      executing_account = 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu'
      AND BYTEARRAY_SUBSTRING(data, 1 + 8, 8) = 0x1bb299ba2fc48c2d
      AND tx_success = TRUE
    UNION ALL
    SELECT
      position_change,
      custody_key,
      amount_out_pre,
      amount_out_post,
      amount_out_pre_usd,
      amount_out_pre_usd / amount_out_pre * amount_out_post AS amount_out_post_usd,
      amount_out_pre_usd - amount_out_pre_usd / amount_out_pre * amount_out_post AS fee_usd,
      block_slot,
      block_time,
      tx_id
    FROM (
      SELECT
        'withdraw' AS position_change,
        TO_BASE58(BYTEARRAY_SUBSTRING(data, 1 + 16, 32)) AS custody_key,
        BYTEARRAY_TO_BIGINT(BYTEARRAY_REVERSE(BYTEARRAY_SUBSTRING(data, 1 + 104, 8))) AS amount_out_pre,
        BYTEARRAY_TO_BIGINT(BYTEARRAY_REVERSE(BYTEARRAY_SUBSTRING(data, 1 + 112, 8))) AS amount_out_post,
        BYTEARRAY_TO_BIGINT(BYTEARRAY_REVERSE(BYTEARRAY_SUBSTRING(data, 1 + 88, 8))) / 1e6 AS amount_out_pre_usd,
        block_slot,
        block_time,
        tx_id
      FROM solana.instruction_calls
      WHERE
        executing_account = 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu'
        AND BYTEARRAY_SUBSTRING(data, 1 + 8, 8) = 0x8dc7b67b9f5ed766
        AND tx_success = TRUE
    )
  )
  SELECT
    DATE_TRUNC('day', block_time) AS day,
    SUM(fee_usd) AS pool_fees
  FROM base
  GROUP BY
    1
), swaps AS (
  SELECT
    DATE_TRUNC('day', block_time) AS day,
    SUM(price * (
      amount_out - amount_out_post_fee
    ) / POWER(10, decimals)) AS swap_fees
  FROM query_3379710
  GROUP BY
    1
)
SELECT
  DATE_TRUNC('day', block_time) AS day,
  p.pool_fees,
  s.swap_fees,
  SUM(fee_usd) AS oc_fees,
  SUM(liq_fee_usd) AS liq_fees,
  COALESCE(p.pool_fees, 0) + 
  COALESCE(s.swap_fees, 0) + 
  COALESCE(SUM(fee_usd), 0) + 
  COALESCE(SUM(liq_fee_usd), 0) AS total_fees,
  jlp.total_fees AS jlp_daily_apr_total_fees
FROM query_3338071 AS q
LEFT JOIN pools AS p
  ON DATE_TRUNC('day', q.block_time) = p.day
LEFT JOIN swaps AS s
  ON DATE_TRUNC('day', q.block_time) = s.day
LEFT JOIN dune.guamacole.result_jlp_daily_apr AS jlp
  ON DATE_TRUNC('day', q.block_time) = jlp.date
GROUP BY
  1, 2, 3, jlp.total_fees
ORDER BY day DESC 