-- USDu Top 100 持币地址：Solana + BSC
WITH solana_holders AS (
  SELECT
    address AS token_balance_owner,
    token_balance,
    cast(date_trunc('day', day) AS date) AS balance_date,
    'solana' AS chain
  FROM solana_utils.daily_balances
  WHERE day = current_date
    AND token_mint_address = '9ckR7pPPvyPadACDTzLwK2ZAEeUJ3qGSnzPs8bVaHrSy'
    AND token_balance >= 1
),

bsc_transfers AS (
  SELECT evt_block_time, "to" AS address, value AS amount
  FROM erc20_bnb.evt_Transfer
  WHERE contract_address = 0xeA953eA6634d55dAC6697C436B1e81A679Db5882
  UNION ALL
  SELECT evt_block_time, "from" AS address, -value AS amount
  FROM erc20_bnb.evt_Transfer
  WHERE contract_address = 0xeA953eA6634d55dAC6697C436B1e81A679Db5882
),

bsc_balance_today AS (
  SELECT
    address,
    sum(amount) AS token_balance
  FROM bsc_transfers
  WHERE evt_block_time <= date_trunc('day', current_date) + interval '1' day
  GROUP BY address
  HAVING sum(amount) >= 1
),

bsc_holders AS (
  SELECT
    concat('0x', to_hex(address)) AS token_balance_owner,
    token_balance,
    cast(current_date AS date) AS balance_date,
    'bsc' AS chain
  FROM bsc_balance_today
),

combined AS (
  SELECT token_balance_owner, token_balance, balance_date, chain FROM solana_holders
  UNION ALL
  SELECT token_balance_owner, token_balance, balance_date, chain FROM bsc_holders
)

SELECT
  token_balance_owner,
  token_balance,
  balance_date,
  chain
FROM combined
ORDER BY token_balance DESC
LIMIT 100;
