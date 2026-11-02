{#
  Đối soát nguồn ↔ fact: giao dịch có trong nguồn nhưng chưa vào fct_transactions,
  hoặc đã vào nhưng trạng thái trong fact khác trạng thái mới nhất của nguồn
  (vd fact còn PENDING trong khi nguồn đã SETTLED).
  Nguyên nhân thường gặp: dữ liệu được nạp song song lúc dbt đang chạy, hoặc có
  dòng được sửa thẳng trong raw mà không đổi _loaded_at.
  Cách xử lý: dbt run -s fct_transactions --vars '{load_lookback_minutes: 1440}'
  (hoặc --full-refresh).
  Mức warn: không chặn pipeline nhưng hiện cảnh báo trong log dbt và Airflow.
#}
{{ config(severity='warn') }}

select
    s.txn_date,
    count(*) filter (where f.txn_id is null) as missing_txn_count,
    count(*) filter (where f.txn_id is not null) as stale_status_count
from {{ ref('stg_core__transactions') }} as s
left join {{ ref('fct_transactions') }} as f
    on s.txn_id = f.txn_id
where f.txn_id is null or f.status <> s.status
group by 1
