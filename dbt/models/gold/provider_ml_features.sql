{{
    config(
        materialized='table',
        unique_key='npi'
    )
}}

/*
    Gold Provider ML Features
    -------------------------
    Advanced features for machine learning fraud detection.
    Includes peer comparisons, concentration metrics, and z-scores.

    Features:
    - Peer comparison (z-scores by specialty + state)
    - Drug concentration (HHI index)
    - Pharma diversity
    - Payment/Prescription ratios
    - Percentile rankings
*/

-- Get provider base metrics
-- Note: SPECIALTY may be NULL, so we use COALESCE for peer grouping
with provider_360 as (
    select
        *,
        COALESCE(SPECIALTY, 'UNKNOWN') as SPECIALTY_GROUP,
        COALESCE(STATE, 'UNKNOWN') as STATE_GROUP
    from {{ ref('provider_360') }}
),

-- Get detailed prescription data for drug concentration
prescriptions as (
    select
        NPI,
        GENERIC_NAME,
        TOTAL_CLAIMS,
        TOTAL_DRUG_COST
    from {{ ref('prescriptions') }}
),

-- Get detailed payment data for pharma diversity
payments as (
    select
        NPI,
        PAYER_NAME,
        PAYMENT_AMOUNT,
        PAYMENT_CATEGORY
    from {{ ref('payments') }}
    where NPI is not null
),

-- Calculate peer statistics (by specialty_group + state_group)
-- Using COALESCE'd groups to handle NULL values
peer_stats as (
    select
        SPECIALTY_GROUP,
        STATE_GROUP,
        COUNT(*) as PEER_COUNT,

        -- Payment stats
        AVG(TOTAL_PAYMENT_AMOUNT) as PEER_AVG_PAYMENT,
        STDDEV(TOTAL_PAYMENT_AMOUNT) as PEER_STDDEV_PAYMENT,
        MEDIAN(TOTAL_PAYMENT_AMOUNT) as PEER_MEDIAN_PAYMENT,

        -- Prescription cost stats
        AVG(TOTAL_PRESCRIPTION_COST) as PEER_AVG_RX_COST,
        STDDEV(TOTAL_PRESCRIPTION_COST) as PEER_STDDEV_RX_COST,
        MEDIAN(TOTAL_PRESCRIPTION_COST) as PEER_MEDIAN_RX_COST,

        -- Brand % stats
        AVG(PCT_BRAND_CLAIMS) as PEER_AVG_BRAND_PCT,
        STDDEV(PCT_BRAND_CLAIMS) as PEER_STDDEV_BRAND_PCT,

        -- Claims stats
        AVG(TOTAL_PRESCRIPTION_CLAIMS) as PEER_AVG_CLAIMS,
        STDDEV(TOTAL_PRESCRIPTION_CLAIMS) as PEER_STDDEV_CLAIMS

    from provider_360
    where TOTAL_PAYMENT_AMOUNT > 0 or TOTAL_PRESCRIPTION_COST > 0
    group by SPECIALTY_GROUP, STATE_GROUP
    having COUNT(*) >= 5  -- Need at least 5 peers for meaningful stats
),

-- Calculate drug concentration (HHI) per provider
-- Step 1: Get claims per drug per provider with totals
drug_claims_with_totals as (
    select
        NPI,
        GENERIC_NAME,
        SUM(TOTAL_CLAIMS) as DRUG_CLAIMS,
        SUM(SUM(TOTAL_CLAIMS)) OVER (PARTITION BY NPI) as PROVIDER_TOTAL_CLAIMS,
        ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY SUM(TOTAL_CLAIMS) DESC) as RN
    from prescriptions
    group by NPI, GENERIC_NAME
),

-- Step 2: Calculate market share and HHI
drug_concentration as (
    select
        NPI,
        COUNT(DISTINCT GENERIC_NAME) as UNIQUE_DRUGS,
        MAX(PROVIDER_TOTAL_CLAIMS) as TOTAL_CLAIMS,

        -- Herfindahl-Hirschman Index (sum of squared market shares)
        -- HHI ranges from 0 (diverse) to 10000 (one drug only)
        SUM(POWER(DRUG_CLAIMS * 100.0 / NULLIF(PROVIDER_TOTAL_CLAIMS, 0), 2)) as DRUG_HHI,

        -- Top drug concentration
        MAX(case when RN = 1 then DRUG_CLAIMS else 0 end) * 100.0 / NULLIF(MAX(PROVIDER_TOTAL_CLAIMS), 0) as TOP_DRUG_PCT,

        -- Top 3 drugs concentration
        SUM(case when RN <= 3 then DRUG_CLAIMS else 0 end) * 100.0 / NULLIF(MAX(PROVIDER_TOTAL_CLAIMS), 0) as TOP_3_DRUGS_PCT

    from drug_claims_with_totals
    group by NPI
),

-- Calculate pharma diversity per provider
pharma_diversity as (
    select
        NPI,
        COUNT(DISTINCT PAYER_NAME) as UNIQUE_PHARMA_COMPANIES,
        COUNT(*) as TOTAL_PAYMENT_RECORDS,

        -- Payment concentration by payer
        MAX(PAYER_TOTAL) * 100.0 / NULLIF(SUM(PAYER_TOTAL), 0) as TOP_PAYER_PCT,

        -- General vs Research payment mix
        SUM(case when PAYMENT_CATEGORY = 'GENERAL' then PAYER_TOTAL else 0 end) * 100.0
            / NULLIF(SUM(PAYER_TOTAL), 0) as PCT_GENERAL_PAYMENTS,
        SUM(case when PAYMENT_CATEGORY = 'RESEARCH' then PAYER_TOTAL else 0 end) * 100.0
            / NULLIF(SUM(PAYER_TOTAL), 0) as PCT_RESEARCH_PAYMENTS

    from (
        select
            NPI,
            PAYER_NAME,
            PAYMENT_CATEGORY,
            SUM(PAYMENT_AMOUNT) as PAYER_TOTAL
        from payments
        group by NPI, PAYER_NAME, PAYMENT_CATEGORY
    )
    group by NPI
),

-- Calculate percentile rankings within specialty_group + state_group
provider_percentiles as (
    select
        NPI,
        SPECIALTY_GROUP,
        STATE_GROUP,

        -- Payment percentile (0-100, higher = more payments)
        PERCENT_RANK() OVER (
            PARTITION BY SPECIALTY_GROUP, STATE_GROUP
            ORDER BY TOTAL_PAYMENT_AMOUNT
        ) * 100 as PAYMENT_PERCENTILE,

        -- Prescription cost percentile
        PERCENT_RANK() OVER (
            PARTITION BY SPECIALTY_GROUP, STATE_GROUP
            ORDER BY TOTAL_PRESCRIPTION_COST
        ) * 100 as RX_COST_PERCENTILE,

        -- Brand % percentile
        PERCENT_RANK() OVER (
            PARTITION BY SPECIALTY_GROUP, STATE_GROUP
            ORDER BY PCT_BRAND_CLAIMS
        ) * 100 as BRAND_PCT_PERCENTILE,

        -- Claims volume percentile
        PERCENT_RANK() OVER (
            PARTITION BY SPECIALTY_GROUP, STATE_GROUP
            ORDER BY TOTAL_PRESCRIPTION_CLAIMS
        ) * 100 as CLAIMS_PERCENTILE

    from provider_360
),

-- Final feature set
final as (
    select
        p.NPI,
        p.FULL_NAME,
        p.ORGANIZATION_NAME,
        p.ENTITY_TYPE,
        p.SPECIALTY,
        p.STATE,

        -- Base metrics (for reference)
        p.TOTAL_PAYMENT_AMOUNT,
        p.TOTAL_PRESCRIPTION_COST,
        p.TOTAL_PRESCRIPTION_CLAIMS,
        p.PCT_BRAND_CLAIMS,
        p.IS_EXCLUDED,

        -- Peer context
        COALESCE(ps.PEER_COUNT, 0) as PEER_COUNT,

        -- Z-SCORES (how many std devs from peer average)
        case
            when ps.PEER_STDDEV_PAYMENT > 0
            then ROUND((p.TOTAL_PAYMENT_AMOUNT - ps.PEER_AVG_PAYMENT) / ps.PEER_STDDEV_PAYMENT, 2)
            else 0
        end as PAYMENT_ZSCORE,

        case
            when ps.PEER_STDDEV_RX_COST > 0
            then ROUND((p.TOTAL_PRESCRIPTION_COST - ps.PEER_AVG_RX_COST) / ps.PEER_STDDEV_RX_COST, 2)
            else 0
        end as RX_COST_ZSCORE,

        case
            when ps.PEER_STDDEV_BRAND_PCT > 0
            then ROUND((p.PCT_BRAND_CLAIMS - ps.PEER_AVG_BRAND_PCT) / ps.PEER_STDDEV_BRAND_PCT, 2)
            else 0
        end as BRAND_PCT_ZSCORE,

        case
            when ps.PEER_STDDEV_CLAIMS > 0
            then ROUND((p.TOTAL_PRESCRIPTION_CLAIMS - ps.PEER_AVG_CLAIMS) / ps.PEER_STDDEV_CLAIMS, 2)
            else 0
        end as CLAIMS_ZSCORE,

        -- RATIOS
        case
            when p.TOTAL_PRESCRIPTION_COST > 0
            then ROUND(p.TOTAL_PAYMENT_AMOUNT / p.TOTAL_PRESCRIPTION_COST, 4)
            else null
        end as PAYMENT_TO_RX_RATIO,

        case
            when p.TOTAL_PRESCRIPTION_CLAIMS > 0
            then ROUND(p.TOTAL_PRESCRIPTION_COST / p.TOTAL_PRESCRIPTION_CLAIMS, 2)
            else null
        end as AVG_COST_PER_CLAIM,

        case
            when ps.PEER_AVG_PAYMENT > 0
            then ROUND(p.TOTAL_PAYMENT_AMOUNT / ps.PEER_AVG_PAYMENT, 2)
            else null
        end as PAYMENT_VS_PEER_RATIO,

        -- DRUG CONCENTRATION
        COALESCE(dc.UNIQUE_DRUGS, 0) as UNIQUE_DRUGS_PRESCRIBED,
        ROUND(COALESCE(dc.DRUG_HHI, 0), 0) as DRUG_CONCENTRATION_HHI,
        ROUND(COALESCE(dc.TOP_DRUG_PCT, 0), 1) as TOP_DRUG_PCT,
        ROUND(COALESCE(dc.TOP_3_DRUGS_PCT, 0), 1) as TOP_3_DRUGS_PCT,

        -- Concentration flag (HHI > 2500 = highly concentrated)
        case when dc.DRUG_HHI > 2500 then true else false end as IS_CONCENTRATED_PRESCRIBER,

        -- PHARMA DIVERSITY
        COALESCE(pd.UNIQUE_PHARMA_COMPANIES, 0) as UNIQUE_PHARMA_COMPANIES,
        ROUND(COALESCE(pd.TOP_PAYER_PCT, 0), 1) as TOP_PAYER_PCT,
        ROUND(COALESCE(pd.PCT_GENERAL_PAYMENTS, 0), 1) as PCT_GENERAL_PAYMENTS,
        ROUND(COALESCE(pd.PCT_RESEARCH_PAYMENTS, 0), 1) as PCT_RESEARCH_PAYMENTS,

        -- Low diversity flag (single payer = more suspicious)
        case when pd.UNIQUE_PHARMA_COMPANIES = 1 then true else false end as IS_SINGLE_PAYER_RECIPIENT,

        -- PERCENTILE RANKINGS
        ROUND(COALESCE(pp.PAYMENT_PERCENTILE, 0), 1) as PAYMENT_PERCENTILE,
        ROUND(COALESCE(pp.RX_COST_PERCENTILE, 0), 1) as RX_COST_PERCENTILE,
        ROUND(COALESCE(pp.BRAND_PCT_PERCENTILE, 0), 1) as BRAND_PCT_PERCENTILE,
        ROUND(COALESCE(pp.CLAIMS_PERCENTILE, 0), 1) as CLAIMS_PERCENTILE,

        -- COMPOSITE ANOMALY FLAGS
        -- Provider is outlier if z-score > 2 in multiple dimensions
        case
            when (
                (case when ps.PEER_STDDEV_PAYMENT > 0 then (p.TOTAL_PAYMENT_AMOUNT - ps.PEER_AVG_PAYMENT) / ps.PEER_STDDEV_PAYMENT else 0 end > 2)::int +
                (case when ps.PEER_STDDEV_RX_COST > 0 then (p.TOTAL_PRESCRIPTION_COST - ps.PEER_AVG_RX_COST) / ps.PEER_STDDEV_RX_COST else 0 end > 2)::int +
                (case when ps.PEER_STDDEV_BRAND_PCT > 0 then (p.PCT_BRAND_CLAIMS - ps.PEER_AVG_BRAND_PCT) / ps.PEER_STDDEV_BRAND_PCT else 0 end > 2)::int +
                (case when ps.PEER_STDDEV_CLAIMS > 0 then (p.TOTAL_PRESCRIPTION_CLAIMS - ps.PEER_AVG_CLAIMS) / ps.PEER_STDDEV_CLAIMS else 0 end > 2)::int
            ) >= 2
            then true else false
        end as IS_MULTI_DIMENSION_OUTLIER,

        -- Count of anomaly flags
        (
            (case when ps.PEER_STDDEV_PAYMENT > 0 then (p.TOTAL_PAYMENT_AMOUNT - ps.PEER_AVG_PAYMENT) / ps.PEER_STDDEV_PAYMENT else 0 end > 2)::int +
            (case when ps.PEER_STDDEV_RX_COST > 0 then (p.TOTAL_PRESCRIPTION_COST - ps.PEER_AVG_RX_COST) / ps.PEER_STDDEV_RX_COST else 0 end > 2)::int +
            (case when ps.PEER_STDDEV_BRAND_PCT > 0 then (p.PCT_BRAND_CLAIMS - ps.PEER_AVG_BRAND_PCT) / ps.PEER_STDDEV_BRAND_PCT else 0 end > 2)::int +
            (COALESCE(dc.DRUG_HHI, 0) > 2500)::int +
            (COALESCE(pd.UNIQUE_PHARMA_COMPANIES, 0) = 1 and p.TOTAL_PAYMENT_AMOUNT > 10000)::int +
            p.IS_EXCLUDED::int
        ) as ANOMALY_FLAG_COUNT,

        current_timestamp() as _loaded_at

    from provider_360 p
    left join peer_stats ps on p.SPECIALTY_GROUP = ps.SPECIALTY_GROUP and p.STATE_GROUP = ps.STATE_GROUP
    left join drug_concentration dc on p.NPI = dc.NPI
    left join pharma_diversity pd on p.NPI = pd.NPI
    left join provider_percentiles pp on p.NPI = pp.NPI
)

select * from final
