with source as (
    select * from {{ source('bronze', 'PROVIDER_INFORMATION') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
