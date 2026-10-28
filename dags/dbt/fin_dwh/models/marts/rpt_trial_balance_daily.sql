{#
  Bảng cân đối số phát sinh theo ngày hạch toán × chi nhánh × loại tiền.
  Tổng phát sinh Nợ phải bằng tổng phát sinh Có -> difference = 0.
  Test assert_trial_balance chặn pipeline nếu có dòng lệch.
#}
select
    posting_date,
    branch_id,
    currency,
    count(distinct journal_id) as journal_count,
    sum(debit_amount) as total_debit,
    sum(credit_amount) as total_credit,
    sum(debit_amount) - sum(credit_amount) as difference
from {{ ref('fct_gl_entries') }}
group by 1, 2, 3
