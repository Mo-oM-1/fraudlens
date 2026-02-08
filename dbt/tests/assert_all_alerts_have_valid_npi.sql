-- Test: All high risk alerts should reference a valid NPI in provider_360
-- Ensures referential integrity between alerts and providers

select
    a.ALERT_ID,
    a.NPI,
    a.ALERT_TYPE
from {{ ref('high_risk_alerts') }} a
left join {{ ref('provider_360') }} p on a.NPI = p.NPI
where p.NPI is null
