{#
  Nguồn là NHẬT KÝ sự kiện: một giao dịch NAPAS có dòng PENDING rồi hôm sau
  có dòng SETTLED/REVERSED. Ở đây chỉ giữ trạng thái MỚI NHẤT của mỗi txn_id.
  ingested_at: thời điểm nguồn gửi sự kiện mới nhất (đồng hồ giả lập).
  loaded_at:   thời điểm warehouse nhận sự kiện gần nhất của txn_id (đồng hồ DB),
               fct_transactions dùng làm mốc incremental.
  Chỉ PARTITION BY txn_id, để điều kiện "txn_id = any(...)" của incremental được
  Postgres đẩy xuống trước khi tính window (dùng index raw.transactions(txn_id)).
#}
with ranked as (
    select
        *,
        max(_loaded_at) over (partition by txn_id) as txn_loaded_at,
        row_number() over (
            partition by txn_id
            order by event_seq desc, ingested_at desc
        ) as rn
    from {{ source('core', 'transactions') }}
)

select
    txn_id,
    account_id,
    branch_id,
    txn_ts,
    txn_date,
    channel,
    direction,
    amount,
    upper(currency) as currency,
    nullif(counterparty_bank, '') as counterparty_bank,
    status,
    ingested_at,
    txn_loaded_at as loaded_at
from ranked
where rn = 1
