WITH sol_raw AS (
  SELECT
    day,
    address,
    token_balance,
    row_number() OVER (PARTITION BY day, address ORDER BY block_time DESC) AS rn
  FROM solana_utils.daily_balances
  WHERE day >= date('2025-06-01')
    AND token_mint_address = '9iq5Q33RSiz1WcupHAQKbHBZkpn92UxBG2HfPWAZhMCa'
),

sol_supply AS (
  SELECT day, sum(token_balance) AS solana_supply
  FROM sol_raw
  WHERE rn = 1
  GROUP BY day
),

bsc_transfers AS (
  SELECT evt_block_time, "to" AS address, value AS amount
  FROM erc20_bnb.evt_Transfer
  WHERE contract_address = 0x385C279445581a186a4182a5503094eBb652EC71
  UNION ALL
  SELECT evt_block_time, "from" AS address, -value AS amount
  FROM erc20_bnb.evt_Transfer
  WHERE contract_address = 0x385C279445581a186a4182a5503094eBb652EC71
),

bsc_daily_changes AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    address,
    sum(amount) AS daily_change
  FROM bsc_transfers
  GROUP BY 1, 2
),

bsc_daily_balance AS (
  SELECT
    day,
    address,
    sum(daily_change) OVER (PARTITION BY address ORDER BY day) AS token_balance
  FROM bsc_daily_changes
),

bsc_supply AS (
  SELECT
    day,
    sum(CASE WHEN token_balance > 0 THEN token_balance ELSE 0 END) AS bsc_supply
  FROM bsc_daily_balance
  GROUP BY day
),

all_days AS (
  SELECT day FROM sol_supply
  UNION
  SELECT day FROM bsc_supply
)

SELECT
  d.day,
  coalesce(s.solana_supply, 0) AS solana_supply,
  coalesce(b.bsc_supply, 0) AS bsc_supply,
  coalesce(s.solana_supply, 0) + coalesce(b.bsc_supply, 0) AS total_supply
FROM all_days d
LEFT JOIN sol_supply s ON s.day = d.day
LEFT JOIN bsc_supply b ON b.day = d.day
ORDER BY d.day DESC;
