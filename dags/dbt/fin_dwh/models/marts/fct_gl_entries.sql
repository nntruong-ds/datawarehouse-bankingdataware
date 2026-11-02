{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='entry_id',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['entry_id'], 'unique': True},
      {'columns': ['posting_date', 'branch_id', 'currency']},
      {'columns': ['loaded_at']}
    ]
  )
}}

{#
  Incremental theo mốc nạp của warehouse (_loaded_at, đồng hồ DB), giống
  fct_transactions. Từng lọc theo max(ingested_at) của nguồn: một bút toán điều
  chỉnh nhập tay với now() (giờ thật, trong khi dữ liệu giả lập ở quá khứ) đã đẩy
  mốc lên tương lai và bảng ngừng nhận bút toán mới. Bút toán đã ghi thì không sửa,
  nên MERGE theo entry_id chỉ để an toàn khi chạy lại.
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
    s.ingested_at,
    s.loaded_at
from {{ ref('stg_core__gl_entries') }} as s
{% if is_incremental() %}
    where s.loaded_at > {{ load_watermark() }}
{% endif %}
