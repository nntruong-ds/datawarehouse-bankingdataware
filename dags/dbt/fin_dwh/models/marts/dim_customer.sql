select
    {{ surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    full_name,
    national_id,
    phone,
    email,
    date_of_birth,
    segment,
    branch_id,
    created_at
from {{ ref('stg_core__customers') }}
