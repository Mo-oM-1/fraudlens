with source as (
    select * from {{ source('bronze', 'OPEN_PAYMENTS_OWNERSHIP') }}
)

select
    *
from source
