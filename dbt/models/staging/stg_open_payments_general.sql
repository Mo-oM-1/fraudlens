with source as (
    select * from {{ source('bronze', 'OPEN_PAYMENTS_GENERAL') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
