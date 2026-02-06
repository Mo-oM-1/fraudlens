with source as (
    select * from AI_FACTORY_DB.BRONZE.OPEN_PAYMENTS_OWNERSHIP
)

select
    *,
    current_timestamp() as _loaded_at
from source