{#
  Nguồn là NHẬT KÝ sự kiện: một giao dịch NAPAS có dòng PENDING rồi hôm sau
  có dòng SETTLED/REVERSED. Ở đây chỉ giữ trạng thái MỚI NHẤT của mỗi txn_id;
  ingested_at là thời điểm nạp của sự kiện mới nhất đó.
#}
with ranked as (
    select
        *,
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
    ingested_at
from ranked
where rn = 1
