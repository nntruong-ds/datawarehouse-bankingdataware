{#
  Nguồn là NHẬT KÝ sự kiện: một giao dịch NAPAS có dòng PENDING rồi hôm sau
  có dòng SETTLED/REVERSED. Ở đây chỉ giữ trạng thái MỚI NHẤT của mỗi txn_id.

  Mẹo hiệu năng: txn_date không đổi trong một txn_id, nên thêm nó vào
  PARTITION BY không đổi kết quả, nhưng cho phép Postgres đẩy điều kiện
  "txn_date >= ..." của model incremental xuống TRƯỚC khi tính window
  -> chỉ quét vài ngày gần nhất thay vì toàn bộ lịch sử.
#}
with ranked as (
    select
        *,
        row_number() over (
            partition by txn_id, txn_date
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
    ingested_at
from ranked
where rn = 1
