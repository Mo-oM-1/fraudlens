with source as (
    select * from {{ source('bronze', 'OPEN_PAYMENTS_RESEARCH') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
