with source as (
    select * from {{ source('bronze', 'OPEN_PAYMENTS_OWNERSHIP') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
