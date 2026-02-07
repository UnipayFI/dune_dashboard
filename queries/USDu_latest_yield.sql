-- Latest yield (Solana + BSC 之和)
WITH requests AS (
  SELECT 'solana' AS source, http_get('https://api.unitas.so/api/usdu/latest_rewards?source=solana') AS body
  UNION ALL
  SELECT 'bsc', http_get('https://api.unitas.so/api/usdu/latest_rewards?source=bsc')
),
parsed AS (
  SELECT
    source,
    TRY_CAST(json_extract_scalar(body, '$.rewards') AS DOUBLE) AS rewards_usdu
  FROM requests
),
totals AS (
  SELECT
    sum(rewards_usdu) AS rewards_usdu,
    max(CASE WHEN source = 'solana' THEN rewards_usdu END) AS rewards_solana,
    max(CASE WHEN source = 'bsc' THEN rewards_usdu END)     AS rewards_bsc
  FROM parsed
)
SELECT
  CAST(date_trunc('week', now()) AS date) AS week_start_utc,
  rewards_usdu,
  rewards_solana,
  rewards_bsc,
  now() AS fetched_at_utc
FROM totals;
