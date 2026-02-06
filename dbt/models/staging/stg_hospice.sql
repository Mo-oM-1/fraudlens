with source as (
    select * from {{ source('bronze', 'HOSPICE') }}
)

select
    *,
    current_timestamp() as _loaded_at
from source
