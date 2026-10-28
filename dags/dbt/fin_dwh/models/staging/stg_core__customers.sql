select
    customer_id,
    trim(full_name) as full_name,
    national_id,
    phone,
    lower(email) as email,
    date_of_birth,
    segment,
    home_branch_id as branch_id,
    created_at
from {{ source('core', 'customers') }}
