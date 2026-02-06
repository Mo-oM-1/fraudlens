with source as (
    select * from {{ source('bronze', 'LONGTERM_CARE_HOSPITAL') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
