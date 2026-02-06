with source as (
    select * from {{ source('nppes', 'DIM_PROVIDER_TAXONOMY') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
