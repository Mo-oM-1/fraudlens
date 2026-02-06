with source as (
    select * from {{ source('nppes', 'DIM_PROVIDER_ADDRESS') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
