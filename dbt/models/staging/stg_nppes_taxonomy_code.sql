with source as (
    select * from {{ source('nppes', 'REF_TAXONOMY_CODE') }}
    where IS_ACTIVE = true
)

select
    *,
    current_timestamp() as _loaded_at
from source
