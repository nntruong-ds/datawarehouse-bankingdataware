select
    branch_id,
    branch_name,
    region
from {{ source('core', 'branches') }}
