select
    account_id,
    customer_id,
    branch_id,
    product_type,
    upper(currency) as currency,
    opened_date,
    status
from {{ source('core', 'accounts') }}
