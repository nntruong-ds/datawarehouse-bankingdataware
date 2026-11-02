{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='txn_id',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['txn_id'], 'unique': True},
      {'columns': ['txn_date']},
      {'columns': ['branch_id']},
      {'columns': ['loaded_at']}
    ]
  )
}}

{#
  Bảng fact giao dịch — INCREMENTAL MERGE theo mốc nạp của warehouse.

  Lần đầu (hoặc --full-refresh): build toàn bộ.
  Các lần sau: chỉ lấy những txn_id có sự kiện mới mà WAREHOUSE nhận (_loaded_at)
  kể từ lần chạy trước, rồi MERGE theo txn_id:
    - txn_id đã có  -> UPDATE (vd PENDING -> SETTLED/REVERSED)
    - txn_id chưa có -> INSERT (giao dịch liên ngân hàng về trễ)
  Mốc là _loaded_at (đồng hồ DB lúc nạp), không phải txn_date hay ingested_at của
  nguồn: giao dịch phát sinh từ 5 ngày trước mà hôm nay mới về / mới quyết toán vẫn
  được lấy, và một dòng nguồn có mốc thời gian lệch không làm mốc nhảy lên tương lai.
  Bảng chỉ xếp hạng lại các txn_id đó (không phải toàn bộ nhật ký), nên lần chạy
  incremental tốn công theo lượng dữ liệu mới, không theo kích thước lịch sử.
  Nếu vẫn sót, test assert_no_late_transactions_missed cảnh báo.
#}

with txn as (
    select * from {{ ref('stg_core__transactions') }}
    {% if is_incremental() %}
        -- "= any(array(...))" (chứ không phải "in (select ...)") để Postgres đẩy điều
        -- kiện xuống trước window của staging và dùng index theo txn_id.
        where txn_id = any(array(
            select distinct r.txn_id
            from {{ source('core', 'transactions') }} as r
            where r._loaded_at > {{ load_watermark() }}
        ))
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
    t.loaded_at,
    current_timestamp as dbt_updated_at
from txn as t
left join {{ ref('stg_core__accounts') }} as a
    on t.account_id = a.account_id
