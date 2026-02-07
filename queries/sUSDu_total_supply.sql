with raw_base as (
  select 
    block_time,
    day, 
    address, 
    token_balance,
    row_number() over (partition by day, address order by block_time desc) as rn
  from solana_utils.daily_balances
  where day >= date('2025-06-01')
    and token_mint_address = '9iq5Q33RSiz1WcupHAQKbHBZkpn92UxBG2HfPWAZhMCa'
),

bsc_transfers as (
  select 
    evt_block_time,
    "to" as address,
    value as amount
  from erc20_bnb.evt_Transfer
  where contract_address = 0x385C279445581a186a4182a5503094eBb652EC71
  union all
  select 
    evt_block_time,
    "from" as address,
    -value as amount
  from erc20_bnb.evt_Transfer
  where contract_address = 0x385C279445581a186a4182a5503094eBb652EC71
),

bsc_daily_changes as (
  select 
    date_trunc('day', evt_block_time) as day,
    address,
    sum(amount) as daily_change
  from bsc_transfers
  group by 1, 2
),

bsc_raw as (
  select
    day,
    concat('0x', to_hex(address)) as address,
    sum(daily_change) over (partition by address order by day) as token_balance
  from bsc_daily_changes
),

raw as (
  select day, address, token_balance from raw_base where rn = 1
  union all
  select day, address, token_balance from bsc_raw
),

addresss as (
  select distinct address from raw 
),

all_dates as (
  select distinct day from solana_utils.daily_balances
  where day between date('2024-02-01') and current_date
),

all_dates_addresss as (
  select 
    day, 
    address 
  from all_dates cross join addresss 
),

daily_join as (
  select 
    day, 
    address, 
    token_balance 
  from all_dates_addresss 
  left join raw using (day, address)
),

final as (
  select 
    day, 
    address, 
    token_balance,
    case 
      when token_balance is null then lag(token_balance) ignore nulls over (partition by address order by day)
      else token_balance
    end as token_balance_fill
  from daily_join 
)

select 
  day, 
  count(1) as total_addresss,
  count(1) - lag(count(1)) over (order by day) as daily_change
from final 
where token_balance_fill > 0
group by day 
order by day desc
