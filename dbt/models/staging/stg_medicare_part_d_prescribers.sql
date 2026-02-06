with source as (
    select * from {{ source('bronze', 'MEDICARE_PART_D_PRESCRIBERS') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
