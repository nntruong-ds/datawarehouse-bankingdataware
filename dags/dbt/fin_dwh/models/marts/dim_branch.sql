select
    {{ surrogate_key(['branch_id']) }} as branch_key,
    branch_id,
    branch_name,
    region
from {{ ref('stg_core__branches') }}
