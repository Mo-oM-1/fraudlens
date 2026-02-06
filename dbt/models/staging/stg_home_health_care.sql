with source as (
    select * from {{ source('bronze', 'HOME_HEALTH_CARE') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
