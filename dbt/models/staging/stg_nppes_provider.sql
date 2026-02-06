with source as (
    select * from {{ source('nppes', 'DIM_PROVIDER') }}
    where IS_ACTIVE = true
)

select
    *,
    current_timestamp() as _loaded_at
from source
