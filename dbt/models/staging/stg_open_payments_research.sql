with source as (
    select * from {{ source('bronze', 'OPEN_PAYMENTS_RESEARCH') }}
)

select
    *
from source
