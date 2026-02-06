with source as (
    select * from AI_FACTORY_DB.BRONZE.HOME_HEALTH_CARE
)

select
    *,
    current_timestamp() as _loaded_at
from source