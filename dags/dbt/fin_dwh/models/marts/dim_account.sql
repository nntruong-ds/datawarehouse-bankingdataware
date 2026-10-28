select
    {{ surrogate_key(['a.account_id']) }} as account_key,
    a.account_id,
    {{ surrogate_key(['a.customer_id']) }} as customer_key,
    a.customer_id,
    a.branch_id,
    a.product_type,
    a.currency,
    a.opened_date,
    a.status
from {{ ref('stg_core__accounts') }} as a
