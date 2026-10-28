{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='txn_id',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['txn_id'], 'unique': True},
      {'columns': ['txn_date']},
      {'columns': ['branch_id']}
    ]
  )
}}

{#
  Bảng fact giao dịch — INCREMENTAL MERGE + QUÉT LÙI.

  Lần đầu (hoặc --full-refresh): build toàn bộ.
  Các lần sau: chỉ đọc giao dịch có txn_date trong N ngày gần nhất
  (N = var lookback_days, mặc định 3) rồi MERGE theo txn_id:
    - txn_id đã có  -> UPDATE (vd PENDING -> SETTLED/REVERSED)
    - txn_id chưa có -> INSERT (giao dịch liên ngân hàng về trễ)
  Giao dịch về trễ HƠN N ngày sẽ bị sót -> test
  assert_no_late_transactions_missed cảnh báo để chạy lại với N lớn hơn.
#}

with txn as (
    select * from {{ ref('stg_core__transactions') }}
    {% if is_incremental() %}
        where txn_date >= (
            select coalesce(max(prev_run.txn_date), '1900-01-01'::date) from {{ this }} as prev_run
        ) - {{ var('lookback_days') | int }}
    {% endif %}
)

select
    t.txn_id,
    cast(to_char(t.txn_date, 'YYYYMMDD') as integer) as date_key,
    t.txn_date,
    t.txn_ts,
    {{ surrogate_key(['t.account_id']) }} as account_key,
    {{ surrogate_key(['a.customer_id']) }} as customer_key,
    {{ surrogate_key(['t.branch_id']) }} as branch_key,
    t.account_id,
    a.customer_id,
    t.branch_id,
    t.channel,
    t.direction,
    t.counterparty_bank,
    t.currency,
    t.amount,
    case when t.direction = 'IN' then t.amount else -t.amount end as amount_signed,
    t.status,
    t.ingested_at,
    current_timestamp as dbt_updated_at
from txn as t
left join {{ ref('stg_core__accounts') }} as a
    on t.account_id = a.account_id
