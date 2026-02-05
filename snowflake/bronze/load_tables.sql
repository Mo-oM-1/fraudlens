-- =============================================================================
-- BRONZE TABLES LOAD
-- Charge les fichiers Parquet depuis S3 vers les tables Bronze
-- =============================================================================

USE DATABASE AI_FACTORY_DB;
USE SCHEMA BRONZE;

-- -----------------------------------------------------------------------------
-- 1. LEIE - Excluded Individuals/Entities
-- -----------------------------------------------------------------------------
COPY INTO LEIE (
    LASTNAME, FIRSTNAME, MIDNAME, BUSNAME, GENERAL, SPECIALTY,
    UPIN, NPI, DOB, ADDRESS, CITY, STATE, ZIP,
    EXCLTYPE, EXCLDATE, REINDATE, WAIVERDATE, WVRSTATE,
    _SOURCE_FILE
)
FROM (
    SELECT
        $1:LASTNAME::VARCHAR,
        $1:FIRSTNAME::VARCHAR,
        $1:MIDNAME::VARCHAR,
        $1:BUSNAME::VARCHAR,
        $1:GENERAL::VARCHAR,
        $1:SPECIALTY::VARCHAR,
        $1:UPIN::VARCHAR,
        $1:NPI::VARCHAR,
        $1:DOB::VARCHAR,
        $1:ADDRESS::VARCHAR,
        $1:CITY::VARCHAR,
        $1:STATE::VARCHAR,
        $1:ZIP::VARCHAR,
        $1:EXCLTYPE::VARCHAR,
        $1:EXCLDATE::VARCHAR,
        $1:REINDATE::VARCHAR,
        $1:WAIVERDATE::VARCHAR,
        $1:WVRSTATE::VARCHAR,
        METADATA$FILENAME
    FROM @BRONZE_S3_STAGE/leie/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*LATEST.*parquet'
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 2. Medicare Hospital Spending by Claim
-- -----------------------------------------------------------------------------
COPY INTO MEDICARE_HOSPITAL_SPENDING (
    FACILITY_ID, FACILITY_NAME, ADDRESS, CITY, STATE, ZIP_CODE, COUNTY_NAME,
    PHONE_NUMBER, CLAIM_TYPE, PERIOD,
    AVG_SPENDING_PER_EPISODE_HOSPITAL, AVG_SPENDING_PER_EPISODE_STATE, AVG_SPENDING_PER_EPISODE_NATION,
    PERCENT_OF_SPENDING_HOSPITAL, PERCENT_OF_SPENDING_STATE, PERCENT_OF_SPENDING_NATION,
    START_DATE, END_DATE, _SOURCE_FILE
)
FROM (
    SELECT
        $1:Facility_ID::VARCHAR,
        $1:Facility_Name::VARCHAR,
        $1:Address::VARCHAR,
        $1:City::VARCHAR,
        $1:State::VARCHAR,
        $1:ZIP_Code::VARCHAR,
        $1:County_Name::VARCHAR,
        $1:Phone_Number::VARCHAR,
        $1:Claim_Type::VARCHAR,
        $1:Period::VARCHAR,
        $1:Avg_Spending_Per_Episode_Hospital::VARCHAR,
        $1:Avg_Spending_Per_Episode_State::VARCHAR,
        $1:Avg_Spending_Per_Episode_Nation::VARCHAR,
        $1:Percent_of_Spending_Hospital::VARCHAR,
        $1:Percent_of_Spending_State::VARCHAR,
        $1:Percent_of_Spending_Nation::VARCHAR,
        $1:Start_Date::VARCHAR,
        $1:End_Date::VARCHAR,
        METADATA$FILENAME
    FROM @BRONZE_S3_STAGE/medicare_hospital_spending/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*LATEST.*parquet'
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 3. Open Payments - General (VARIANT pour schema flexible)
-- -----------------------------------------------------------------------------
COPY INTO OPEN_PAYMENTS_GENERAL (DATA, _SOURCE_FILE)
FROM (
    SELECT
        $1,
        METADATA$FILENAME
    FROM @BRONZE_S3_STAGE/open_payments/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*General.*LATEST.*parquet'
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 4. Open Payments - Research (VARIANT)
-- -----------------------------------------------------------------------------
COPY INTO OPEN_PAYMENTS_RESEARCH (DATA, _SOURCE_FILE)
FROM (
    SELECT
        $1,
        METADATA$FILENAME
    FROM @BRONZE_S3_STAGE/open_payments/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*Research.*LATEST.*parquet'
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 5. Open Payments - Ownership (VARIANT)
-- -----------------------------------------------------------------------------
COPY INTO OPEN_PAYMENTS_OWNERSHIP (DATA, _SOURCE_FILE)
FROM (
    SELECT
        $1,
        METADATA$FILENAME
    FROM @BRONZE_S3_STAGE/open_payments/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*Ownership.*LATEST.*parquet'
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 6. Provider Information (Nursing Home)
-- -----------------------------------------------------------------------------
COPY INTO PROVIDER_INFORMATION (
    FEDERAL_PROVIDER_NUMBER, PROVIDER_NAME, PROVIDER_ADDRESS, PROVIDER_CITY,
    PROVIDER_STATE, PROVIDER_ZIP_CODE, PROVIDER_PHONE_NUMBER,
    PROVIDER_SSA_COUNTY_CODE, PROVIDER_COUNTY_NAME, OWNERSHIP_TYPE,
    NUMBER_OF_CERTIFIED_BEDS, NUMBER_OF_RESIDENTS, AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY,
    PROVIDER_TYPE, PROVIDER_RESIDES_IN_HOSPITAL, LEGAL_BUSINESS_NAME,
    DATE_FIRST_APPROVED_TO_PROVIDE_MEDICARE_AND_MEDICAID_SERVICES,
    CONTINUING_CARE_RETIREMENT_COMMUNITY, SPECIAL_FOCUS_FACILITY,
    OVERALL_RATING, HEALTH_INSPECTION_RATING, STAFFING_RATING, QM_RATING,
    LOCATION, PROCESSING_DATE, _SOURCE_FILE
)
FROM (
    SELECT
        $1:"Federal Provider Number"::VARCHAR,
        $1:"Provider Name"::VARCHAR,
        $1:"Provider Address"::VARCHAR,
        $1:"Provider City"::VARCHAR,
        $1:"Provider State"::VARCHAR,
        $1:"Provider Zip Code"::VARCHAR,
        $1:"Provider Phone Number"::VARCHAR,
        $1:"Provider SSA County Code"::VARCHAR,
        $1:"Provider County Name"::VARCHAR,
        $1:"Ownership Type"::VARCHAR,
        $1:"Number of Certified Beds"::VARCHAR,
        $1:"Number of Residents"::VARCHAR,
        $1:"Average Number of Residents per Day"::VARCHAR,
        $1:"Provider Type"::VARCHAR,
        $1:"Provider Resides in Hospital"::VARCHAR,
        $1:"Legal Business Name"::VARCHAR,
        $1:"Date First Approved to Provide Medicare and Medicaid Services"::VARCHAR,
        $1:"Continuing Care Retirement Community"::VARCHAR,
        $1:"Special Focus Facility"::VARCHAR,
        $1:"Overall Rating"::VARCHAR,
        $1:"Health Inspection Rating"::VARCHAR,
        $1:"Staffing Rating"::VARCHAR,
        $1:"QM Rating"::VARCHAR,
        $1:"Location"::VARCHAR,
        $1:"Processing Date"::VARCHAR,
        METADATA$FILENAME
    FROM @BRONZE_S3_STAGE/provider_information/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*LATEST.*parquet'
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 7. Long-Term Care Hospital
-- -----------------------------------------------------------------------------
COPY INTO LONGTERM_CARE_HOSPITAL (
    CMS_CERTIFICATION_NUMBER, FACILITY_NAME, ADDRESS, CITY, STATE, ZIP_CODE,
    COUNTY_NAME, PHONE_NUMBER, CMS_REGION, MEASURE_CODE, SCORE, FOOTNOTE,
    START_DATE, END_DATE, _SOURCE_FILE
)
FROM (
    SELECT
        $1:"CMS Certification Number"::VARCHAR,
        $1:"Facility Name"::VARCHAR,
        $1:"Address"::VARCHAR,
        $1:"City"::VARCHAR,
        $1:"State"::VARCHAR,
        $1:"ZIP Code"::VARCHAR,
        $1:"County Name"::VARCHAR,
        $1:"Phone Number"::VARCHAR,
        $1:"CMS Region"::VARCHAR,
        $1:"Measure Code"::VARCHAR,
        $1:"Score"::VARCHAR,
        $1:"Footnote"::VARCHAR,
        $1:"Start Date"::VARCHAR,
        $1:"End Date"::VARCHAR,
        METADATA$FILENAME
    FROM @BRONZE_S3_STAGE/longterm_care_hospital/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*LATEST.*parquet'
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 8. Hospice
-- -----------------------------------------------------------------------------
COPY INTO HOSPICE (
    CMS_CERTIFICATION_NUMBER, FACILITY_NAME, ADDRESS_LINE_1, ADDRESS_LINE_2,
    CITY, STATE, ZIP_CODE, COUNTY_NAME, PHONE_NUMBER, CMS_REGION,
    OWNERSHIP_TYPE, CERTIFICATION_DATE, _SOURCE_FILE
)
FROM (
    SELECT
        $1:"CMS Certification Number (CCN)"::VARCHAR,
        $1:"Facility Name"::VARCHAR,
        $1:"Address Line 1"::VARCHAR,
        $1:"Address Line 2"::VARCHAR,
        $1:"City"::VARCHAR,
        $1:"State"::VARCHAR,
        $1:"ZIP Code"::VARCHAR,
        $1:"County Name"::VARCHAR,
        $1:"Phone Number"::VARCHAR,
        $1:"CMS Region"::VARCHAR,
        $1:"Ownership Type"::VARCHAR,
        $1:"Certification Date"::VARCHAR,
        METADATA$FILENAME
    FROM @BRONZE_S3_STAGE/hospice/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*LATEST.*parquet'
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 9. Home Health Care
-- -----------------------------------------------------------------------------
COPY INTO HOME_HEALTH_CARE (
    CMS_CERTIFICATION_NUMBER, PROVIDER_NAME, ADDRESS, CITY, STATE, ZIP_CODE,
    PHONE, TYPE_OF_OWNERSHIP, OFFERS_NURSING_CARE, OFFERS_PHYSICAL_THERAPY,
    OFFERS_OCCUPATIONAL_THERAPY, OFFERS_SPEECH_PATHOLOGY,
    OFFERS_MEDICAL_SOCIAL_SERVICES, OFFERS_HOME_HEALTH_AIDE_SERVICES,
    DATE_CERTIFIED, _SOURCE_FILE
)
FROM (
    SELECT
        $1:"CMS Certification Number (CCN)"::VARCHAR,
        $1:"Provider Name"::VARCHAR,
        $1:"Address"::VARCHAR,
        $1:"City"::VARCHAR,
        $1:"State"::VARCHAR,
        $1:"ZIP Code"::VARCHAR,
        $1:"Phone"::VARCHAR,
        $1:"Type of Ownership"::VARCHAR,
        $1:"Offers Nursing Care Services"::VARCHAR,
        $1:"Offers Physical Therapy Services"::VARCHAR,
        $1:"Offers Occupational Therapy Services"::VARCHAR,
        $1:"Offers Speech Pathology Services"::VARCHAR,
        $1:"Offers Medical Social Services"::VARCHAR,
        $1:"Offers Home Health Aide Services"::VARCHAR,
        $1:"Date Certified"::VARCHAR,
        METADATA$FILENAME
    FROM @BRONZE_S3_STAGE/home_health_care/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*LATEST.*parquet'
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 10. Medicare Part D Prescribers
-- -----------------------------------------------------------------------------
COPY INTO MEDICARE_PART_D_PRESCRIBERS (
    PRSCRBR_NPI, PRSCRBR_LAST_ORG_NAME, PRSCRBR_FIRST_NAME,
    PRSCRBR_CITY, PRSCRBR_STATE_ABRVTN, PRSCRBR_STATE_FIPS,
    PRSCRBR_TYPE, PRSCRBR_TYPE_SRC, BRND_NAME, GNRC_NAME,
    TOT_CLMS, TOT_30DAY_FILLS, TOT_DAY_SUPLY, TOT_DRUG_CST, TOT_BENES,
    GE65_SPRSN_FLAG, GE65_TOT_CLMS, GE65_TOT_30DAY_FILLS,
    GE65_TOT_DAY_SUPLY, GE65_TOT_DRUG_CST, GE65_BENE_SPRSN_FLAG, GE65_TOT_BENES,
    _SOURCE_FILE
)
FROM (
    SELECT
        $1:Prscrbr_NPI::VARCHAR,
        $1:Prscrbr_Last_Org_Name::VARCHAR,
        $1:Prscrbr_First_Name::VARCHAR,
        $1:Prscrbr_City::VARCHAR,
        $1:Prscrbr_State_Abrvtn::VARCHAR,
        $1:Prscrbr_State_FIPS::VARCHAR,
        $1:Prscrbr_Type::VARCHAR,
        $1:Prscrbr_Type_Src::VARCHAR,
        $1:Brnd_Name::VARCHAR,
        $1:Gnrc_Name::VARCHAR,
        $1:Tot_Clms::VARCHAR,
        $1:Tot_30day_Fills::VARCHAR,
        $1:Tot_Day_Suply::VARCHAR,
        $1:Tot_Drug_Cst::VARCHAR,
        $1:Tot_Benes::VARCHAR,
        $1:GE65_Sprsn_Flag::VARCHAR,
        $1:GE65_Tot_Clms::VARCHAR,
        $1:GE65_Tot_30day_Fills::VARCHAR,
        $1:GE65_Tot_Day_Suply::VARCHAR,
        $1:GE65_Tot_Drug_Cst::VARCHAR,
        $1:GE65_Bene_Sprsn_Flag::VARCHAR,
        $1:GE65_Tot_Benes::VARCHAR,
        METADATA$FILENAME
    FROM @BRONZE_S3_STAGE/medicare_part_d_prescribers/
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*LATEST.*parquet'
ON_ERROR = 'CONTINUE';
