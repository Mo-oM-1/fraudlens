with source as (
    select * from {{ source('bronze', 'OPEN_PAYMENTS_GENERAL') }}
)

select
    *
from source
