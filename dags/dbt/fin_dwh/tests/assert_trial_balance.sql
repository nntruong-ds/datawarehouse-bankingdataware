{#
  CIRCUIT BREAKER — bảng cân đối số phát sinh phải cân.
  Với mỗi ngày hạch toán × chi nhánh × loại tiền: tổng phát sinh Nợ = tổng phát sinh Có.

  Test đọc stg_core__gl_entries, tức chặn ngay ở cửa vào: test FAIL -> dbt build /
  Cosmos đánh dấu lỗi -> fct_gl_entries và các báo cáo phía sau
  (rpt_trial_balance_daily, rpt_npl_ratio_daily) bị SKIP. Bảng fact, view secure.*
  và báo cáo giữ nguyên số của lần chạy đúng gần nhất, không ai đọc được số sai.
  (Nếu đặt test trên fct_gl_entries thì fact và view secure.fct_gl_entries đã kịp
  nhận số sai trước khi test chạy.)
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
from {{ ref('stg_core__gl_entries') }}
group by 1, 2, 3
having sum(debit_amount) <> sum(credit_amount)
