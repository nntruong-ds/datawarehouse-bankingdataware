select
    loan_id,
    customer_id,
    branch_id,
    principal,
    currency,
    disbursed_date,
    debt_group,
    debt_group >= 3 as is_npl,
    classified_date,
    updated_at
from {{ source('core', 'loans') }}
