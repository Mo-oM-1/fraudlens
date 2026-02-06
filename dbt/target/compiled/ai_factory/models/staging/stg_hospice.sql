with source as (
    select * from AI_FACTORY_DB.BRONZE.HOSPICE
)

select
    *,
    current_timestamp() as _loaded_at
from source