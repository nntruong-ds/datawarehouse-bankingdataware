{#
  Dimension SCD Type 2 cho khoản vay, đọc từ snapshot.
  Muốn biết "ngày X khoản vay thuộc nhóm nợ nào":
    where X >= valid_from and X < valid_to
#}
select
    {{ surrogate_key(['loan_id', 'dbt_valid_from']) }} as loan_version_key,
    loan_id,
    customer_id,
    branch_id,
    principal,
    currency,
    disbursed_date,
    debt_group,
    debt_group >= 3 as is_npl,
    classified_date,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to = '9999-12-31'::timestamp as is_current
from {{ ref('snap_loans') }}
