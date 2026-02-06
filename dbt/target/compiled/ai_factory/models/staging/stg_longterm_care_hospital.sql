with source as (
    select * from AI_FACTORY_DB.BRONZE.LONGTERM_CARE_HOSPITAL
)

select
    *,
    current_timestamp() as _loaded_at
from source