with source as (
    select * from {{ source('bronze', 'MEDICARE_HOSPITAL_SPENDING') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
