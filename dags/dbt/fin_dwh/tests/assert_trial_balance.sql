{#
  CIRCUIT BREAKER — bảng cân đối số phát sinh phải cân.
  Với mỗi ngày hạch toán × chi nhánh × loại tiền: tổng phát sinh Nợ = tổng phát sinh Có.

  Test đọc trực tiếp fct_gl_entries nên được gắn vào model đó:
  test FAIL -> dbt build / Cosmos đánh dấu lỗi -> các model phía sau
  (rpt_trial_balance_daily, rpt_npl_ratio_daily) bị SKIP, báo cáo không bị cập nhật
  bằng số liệu sai.
  So sánh chính xác (numeric, không dùng float) vì số tiền kế toán phải khớp tuyệt đối.
#}
{{ config(severity='error', tags=['circuit_breaker']) }}

select
    posting_date,
    branch_id,
    currency,
    sum(debit_amount) as total_debit,
    sum(credit_amount) as total_credit,
    sum(debit_amount) - sum(credit_amount) as difference
from {{ ref('fct_gl_entries') }}
group by 1, 2, 3
having sum(debit_amount) <> sum(credit_amount)
