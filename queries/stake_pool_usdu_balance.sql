-- Stake pool USDu balance: Solana 指定地址 + BSC 合约总余额
WITH solana_balance AS (
  SELECT
    day,
    token_balance AS solana_usdu_balance
  FROM solana_utils.daily_balances
  WHERE address = 'CFgrWjb9DYKVqf7QyQfmwjboDDkXpFHQ6292rnYxrjsa'
    AND token_mint_address = '9ckR7pPPvyPadACDTzLwK2ZAEeUJ3qGSnzPs8bVaHrSy'
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

bsc_total_per_day AS (
  SELECT
    day,
    sum(CASE WHEN token_balance > 0 THEN token_balance ELSE 0 END) AS bsc_usdu_balance
  FROM bsc_daily_balance
  GROUP BY day
),

all_days AS (
  SELECT day FROM solana_balance
  UNION
  SELECT day FROM bsc_total_per_day
)

SELECT
  d.day,
  s.solana_usdu_balance,
  b.bsc_usdu_balance,
  coalesce(s.solana_usdu_balance, 0) + coalesce(b.bsc_usdu_balance, 0) AS total_usdu_balance
FROM all_days d
LEFT JOIN solana_balance s ON s.day = d.day
LEFT JOIN bsc_total_per_day b ON b.day = d.day
ORDER BY d.day DESC;
