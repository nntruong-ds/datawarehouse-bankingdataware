{#
  Đối soát nguồn ↔ fact: giao dịch có trong nguồn nhưng chưa vào fct_transactions.
  Nguyên nhân thường gặp: giao dịch về trễ hơn cửa sổ lookback_days.
  Cách xử lý: dbt run -s fct_transactions --vars '{lookback_days: 7}'
  Mức warn: không chặn pipeline nhưng hiện cảnh báo + lưu vào Elementary.
#}
{{ config(severity='warn') }}

select
    s.txn_date,
    count(*) as missing_txn_count
from {{ ref('stg_core__transactions') }} as s
left join {{ ref('fct_transactions') }} as f
    on s.txn_id = f.txn_id
where f.txn_id is null
group by 1
