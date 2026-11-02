{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='entry_id',
    indexes=[
      {'columns': ['entry_id'], 'unique': True},
      {'columns': ['posting_date', 'branch_id', 'currency']}
    ]
  )
}}

select
    entry_id,
    journal_id,
    posting_date,
    cast(to_char(posting_date, 'YYYYMMDD') as integer) as date_key,
    {{ surrogate_key(['branch_id']) }} as branch_key,
    branch_id,
    gl_account,
    currency,
    debit_amount,
    credit_amount,
    source_txn_id,
    ingested_at
from {{ ref('stg_core__gl_entries') }}
{% if is_incremental() %}
    -- Như fct_transactions: lọc theo thời điểm nạp, để bút toán bổ sung về trễ
    -- (posting_date cũ) vẫn được lấy.
    where ingested_at >= (
        select coalesce(max(prev_run.ingested_at), '1900-01-01'::timestamp) from {{ this }} as prev_run
    ) - interval '{{ var('lookback_days') | int }} days'
{% endif %}
