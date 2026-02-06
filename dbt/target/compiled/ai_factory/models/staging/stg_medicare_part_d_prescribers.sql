with source as (
    select * from AI_FACTORY_DB.BRONZE.MEDICARE_PART_D_PRESCRIBERS
)

select
    *,
    current_timestamp() as _loaded_at
from source