with source as (
    select * from AI_FACTORY_DB.BRONZE.MEDICARE_HOSPITAL_SPENDING
)

select
    *,
    current_timestamp() as _loaded_at
from source