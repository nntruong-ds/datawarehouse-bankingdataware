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

{#
  Bút toán đã ghi thì không bao giờ sửa (muốn điều chỉnh thì ghi bút toán mới),
  nên incremental chỉ cần lấy các entry_id CHƯA có trong bảng, không cần mốc
  thời gian. Lọc theo max(ingested_at) thì một dòng có mốc thời gian lệch (vd bút
  toán điều chỉnh nhập tay với now()) sẽ làm mọi bút toán mới sau đó bị bỏ qua.
  Test assert_gl_entries_reconciled đối soát staging ↔ fact để bắt lỗi kiểu này.
#}
select
    s.entry_id,
    s.journal_id,
    s.posting_date,
    cast(to_char(s.posting_date, 'YYYYMMDD') as integer) as date_key,
    {{ surrogate_key(['s.branch_id']) }} as branch_key,
    s.branch_id,
    s.gl_account,
    s.currency,
    s.debit_amount,
    s.credit_amount,
    s.source_txn_id,
    s.ingested_at
from {{ ref('stg_core__gl_entries') }} as s
{% if is_incremental() %}
    where not exists (
        select 1 from {{ this }} as prev_run
        where prev_run.entry_id = s.entry_id
    )
{% endif %}
