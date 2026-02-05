from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# DAG DEFAULTS
# -------------------------------------------------------------------
default_args = {
    'owner': 'MooM',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
with DAG(
    dag_id='load_bronze_tables',
    default_args=default_args,
    schedule=None,
    catchup=False,
    doc_md="""
    # Load Bronze Tables DAG

    Charge les fichiers Parquet depuis S3 vers les tables Bronze dans Snowflake.

    **Etapes:**
    1. Creer le stage S3 pour Bronze
    2. Creer les tables Bronze (si non existantes)
    3. Truncate les tables existantes (full refresh)
    4. Charger les donnees depuis S3
    5. Afficher le resume
    """,
    tags=['snowflake', 'bronze', 'load'],
) as dag:

    # -------------------------------------------------------------------
    # TASK 1 : Create Bronze Tables
    # -------------------------------------------------------------------
    create_tables = SnowflakeSqlApiOperator(
        task_id='create_bronze_tables',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

            -- LEIE
            CREATE TABLE IF NOT EXISTS LEIE (
                LASTNAME VARCHAR, FIRSTNAME VARCHAR, MIDNAME VARCHAR, BUSNAME VARCHAR,
                GENERAL VARCHAR, SPECIALTY VARCHAR, UPIN VARCHAR, NPI VARCHAR, DOB VARCHAR,
                ADDRESS VARCHAR, CITY VARCHAR, STATE VARCHAR, ZIP VARCHAR,
                EXCLTYPE VARCHAR, EXCLDATE VARCHAR, REINDATE VARCHAR, WAIVERDATE VARCHAR, WVRSTATE VARCHAR,
                _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _SOURCE_FILE VARCHAR
            );

            -- Medicare Hospital Spending
            CREATE TABLE IF NOT EXISTS MEDICARE_HOSPITAL_SPENDING (
                FACILITY_ID VARCHAR, FACILITY_NAME VARCHAR, ADDRESS VARCHAR, CITY VARCHAR,
                STATE VARCHAR, ZIP_CODE VARCHAR, COUNTY_NAME VARCHAR, PHONE_NUMBER VARCHAR,
                CLAIM_TYPE VARCHAR, PERIOD VARCHAR,
                AVG_SPENDING_PER_EPISODE_HOSPITAL VARCHAR, AVG_SPENDING_PER_EPISODE_STATE VARCHAR,
                AVG_SPENDING_PER_EPISODE_NATION VARCHAR, PERCENT_OF_SPENDING_HOSPITAL VARCHAR,
                PERCENT_OF_SPENDING_STATE VARCHAR, PERCENT_OF_SPENDING_NATION VARCHAR,
                START_DATE VARCHAR, END_DATE VARCHAR,
                _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _SOURCE_FILE VARCHAR
            );

            -- Open Payments (VARIANT pour schemas flexibles)
            CREATE TABLE IF NOT EXISTS OPEN_PAYMENTS_GENERAL (
                DATA VARIANT, _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _SOURCE_FILE VARCHAR
            );
            CREATE TABLE IF NOT EXISTS OPEN_PAYMENTS_RESEARCH (
                DATA VARIANT, _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _SOURCE_FILE VARCHAR
            );
            CREATE TABLE IF NOT EXISTS OPEN_PAYMENTS_OWNERSHIP (
                DATA VARIANT, _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _SOURCE_FILE VARCHAR
            );

            -- Provider Information
            CREATE TABLE IF NOT EXISTS PROVIDER_INFORMATION (
                FEDERAL_PROVIDER_NUMBER VARCHAR, PROVIDER_NAME VARCHAR, PROVIDER_ADDRESS VARCHAR,
                PROVIDER_CITY VARCHAR, PROVIDER_STATE VARCHAR, PROVIDER_ZIP_CODE VARCHAR,
                PROVIDER_PHONE_NUMBER VARCHAR, PROVIDER_SSA_COUNTY_CODE VARCHAR, PROVIDER_COUNTY_NAME VARCHAR,
                OWNERSHIP_TYPE VARCHAR, NUMBER_OF_CERTIFIED_BEDS VARCHAR, NUMBER_OF_RESIDENTS VARCHAR,
                AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY VARCHAR, PROVIDER_TYPE VARCHAR,
                PROVIDER_RESIDES_IN_HOSPITAL VARCHAR, LEGAL_BUSINESS_NAME VARCHAR,
                DATE_FIRST_APPROVED VARCHAR, CONTINUING_CARE_RETIREMENT_COMMUNITY VARCHAR,
                SPECIAL_FOCUS_FACILITY VARCHAR, OVERALL_RATING VARCHAR, HEALTH_INSPECTION_RATING VARCHAR,
                STAFFING_RATING VARCHAR, QM_RATING VARCHAR, LOCATION VARCHAR, PROCESSING_DATE VARCHAR,
                _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _SOURCE_FILE VARCHAR
            );

            -- Long-Term Care Hospital
            CREATE TABLE IF NOT EXISTS LONGTERM_CARE_HOSPITAL (
                CMS_CERTIFICATION_NUMBER VARCHAR, FACILITY_NAME VARCHAR, ADDRESS VARCHAR,
                CITY VARCHAR, STATE VARCHAR, ZIP_CODE VARCHAR, COUNTY_NAME VARCHAR,
                PHONE_NUMBER VARCHAR, CMS_REGION VARCHAR, MEASURE_CODE VARCHAR, SCORE VARCHAR,
                FOOTNOTE VARCHAR, START_DATE VARCHAR, END_DATE VARCHAR,
                _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _SOURCE_FILE VARCHAR
            );

            -- Hospice
            CREATE TABLE IF NOT EXISTS HOSPICE (
                CMS_CERTIFICATION_NUMBER VARCHAR, FACILITY_NAME VARCHAR, ADDRESS_LINE_1 VARCHAR,
                ADDRESS_LINE_2 VARCHAR, CITY VARCHAR, STATE VARCHAR, ZIP_CODE VARCHAR,
                COUNTY_NAME VARCHAR, PHONE_NUMBER VARCHAR, CMS_REGION VARCHAR,
                OWNERSHIP_TYPE VARCHAR, CERTIFICATION_DATE VARCHAR,
                _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _SOURCE_FILE VARCHAR
            );

            -- Home Health Care
            CREATE TABLE IF NOT EXISTS HOME_HEALTH_CARE (
                CMS_CERTIFICATION_NUMBER VARCHAR, PROVIDER_NAME VARCHAR, ADDRESS VARCHAR,
                CITY VARCHAR, STATE VARCHAR, ZIP_CODE VARCHAR, PHONE VARCHAR,
                TYPE_OF_OWNERSHIP VARCHAR, OFFERS_NURSING_CARE VARCHAR, OFFERS_PHYSICAL_THERAPY VARCHAR,
                OFFERS_OCCUPATIONAL_THERAPY VARCHAR, OFFERS_SPEECH_PATHOLOGY VARCHAR,
                OFFERS_MEDICAL_SOCIAL_SERVICES VARCHAR, OFFERS_HOME_HEALTH_AIDE_SERVICES VARCHAR,
                DATE_CERTIFIED VARCHAR,
                _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _SOURCE_FILE VARCHAR
            );

            -- Medicare Part D Prescribers
            CREATE TABLE IF NOT EXISTS MEDICARE_PART_D_PRESCRIBERS (
                PRSCRBR_NPI VARCHAR, PRSCRBR_LAST_ORG_NAME VARCHAR, PRSCRBR_FIRST_NAME VARCHAR,
                PRSCRBR_CITY VARCHAR, PRSCRBR_STATE_ABRVTN VARCHAR, PRSCRBR_STATE_FIPS VARCHAR,
                PRSCRBR_TYPE VARCHAR, PRSCRBR_TYPE_SRC VARCHAR, BRND_NAME VARCHAR, GNRC_NAME VARCHAR,
                TOT_CLMS VARCHAR, TOT_30DAY_FILLS VARCHAR, TOT_DAY_SUPLY VARCHAR, TOT_DRUG_CST VARCHAR,
                TOT_BENES VARCHAR, GE65_SPRSN_FLAG VARCHAR, GE65_TOT_CLMS VARCHAR, GE65_TOT_30DAY_FILLS VARCHAR,
                GE65_TOT_DAY_SUPLY VARCHAR, GE65_TOT_DRUG_CST VARCHAR, GE65_BENE_SPRSN_FLAG VARCHAR,
                GE65_TOT_BENES VARCHAR,
                _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), _SOURCE_FILE VARCHAR
            );
        """,
        statement_count=12,
    )

    # -------------------------------------------------------------------
    # TASK 2 : Truncate Tables (Full Refresh)
    # -------------------------------------------------------------------
    truncate_tables = SnowflakeSqlApiOperator(
        task_id='truncate_bronze_tables',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

            TRUNCATE TABLE IF EXISTS LEIE;
            TRUNCATE TABLE IF EXISTS MEDICARE_HOSPITAL_SPENDING;
            TRUNCATE TABLE IF EXISTS OPEN_PAYMENTS_GENERAL;
            TRUNCATE TABLE IF EXISTS OPEN_PAYMENTS_RESEARCH;
            TRUNCATE TABLE IF EXISTS OPEN_PAYMENTS_OWNERSHIP;
            TRUNCATE TABLE IF EXISTS PROVIDER_INFORMATION;
            TRUNCATE TABLE IF EXISTS LONGTERM_CARE_HOSPITAL;
            TRUNCATE TABLE IF EXISTS HOSPICE;
            TRUNCATE TABLE IF EXISTS HOME_HEALTH_CARE;
            TRUNCATE TABLE IF EXISTS MEDICARE_PART_D_PRESCRIBERS;
        """,
        statement_count=12,
    )

    # -------------------------------------------------------------------
    # TASK 3 : Load LEIE
    # -------------------------------------------------------------------
    load_leie = SnowflakeSqlApiOperator(
        task_id='load_leie',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

            COPY INTO LEIE (
                LASTNAME, FIRSTNAME, MIDNAME, BUSNAME, GENERAL, SPECIALTY,
                UPIN, NPI, DOB, ADDRESS, CITY, STATE, ZIP,
                EXCLTYPE, EXCLDATE, REINDATE, WAIVERDATE, WVRSTATE, _SOURCE_FILE
            )
            FROM (
                SELECT
                    $1:LASTNAME::VARCHAR, $1:FIRSTNAME::VARCHAR, $1:MIDNAME::VARCHAR,
                    $1:BUSNAME::VARCHAR, $1:GENERAL::VARCHAR, $1:SPECIALTY::VARCHAR,
                    $1:UPIN::VARCHAR, $1:NPI::VARCHAR, $1:DOB::VARCHAR,
                    $1:ADDRESS::VARCHAR, $1:CITY::VARCHAR, $1:STATE::VARCHAR, $1:ZIP::VARCHAR,
                    $1:EXCLTYPE::VARCHAR, $1:EXCLDATE::VARCHAR, $1:REINDATE::VARCHAR,
                    $1:WAIVERDATE::VARCHAR, $1:WVRSTATE::VARCHAR, METADATA$FILENAME
                FROM @BRONZE_S3_STAGE/leie/
            )
            FILE_FORMAT = (TYPE = PARQUET)
            PATTERN = '.*LATEST.*parquet'
            ON_ERROR = 'CONTINUE';
        """,
        statement_count=3,
    )

    # -------------------------------------------------------------------
    # TASK 4 : Load Medicare Hospital Spending
    # -------------------------------------------------------------------
    load_medicare_spending = SnowflakeSqlApiOperator(
        task_id='load_medicare_hospital_spending',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

            COPY INTO MEDICARE_HOSPITAL_SPENDING (
                FACILITY_ID, FACILITY_NAME, ADDRESS, CITY, STATE, ZIP_CODE, COUNTY_NAME,
                PHONE_NUMBER, CLAIM_TYPE, PERIOD,
                AVG_SPENDING_PER_EPISODE_HOSPITAL, AVG_SPENDING_PER_EPISODE_STATE,
                AVG_SPENDING_PER_EPISODE_NATION, PERCENT_OF_SPENDING_HOSPITAL,
                PERCENT_OF_SPENDING_STATE, PERCENT_OF_SPENDING_NATION,
                START_DATE, END_DATE, _SOURCE_FILE
            )
            FROM (
                SELECT
                    $1:Facility_ID::VARCHAR, $1:Facility_Name::VARCHAR, $1:Address::VARCHAR,
                    $1:City::VARCHAR, $1:State::VARCHAR, $1:ZIP_Code::VARCHAR, $1:County_Name::VARCHAR,
                    $1:Phone_Number::VARCHAR, $1:Claim_Type::VARCHAR, $1:Period::VARCHAR,
                    $1:Avg_Spending_Per_Episode_Hospital::VARCHAR, $1:Avg_Spending_Per_Episode_State::VARCHAR,
                    $1:Avg_Spending_Per_Episode_Nation::VARCHAR, $1:Percent_of_Spending_Hospital::VARCHAR,
                    $1:Percent_of_Spending_State::VARCHAR, $1:Percent_of_Spending_Nation::VARCHAR,
                    $1:Start_Date::VARCHAR, $1:End_Date::VARCHAR, METADATA$FILENAME
                FROM @BRONZE_S3_STAGE/medicare_hospital_spending/
            )
            FILE_FORMAT = (TYPE = PARQUET)
            PATTERN = '.*LATEST.*parquet'
            ON_ERROR = 'CONTINUE';
        """,
        statement_count=3,
    )

    # -------------------------------------------------------------------
    # TASK 5 : Load Open Payments General
    # -------------------------------------------------------------------
    load_open_payments_general = SnowflakeSqlApiOperator(
        task_id='load_open_payments_general',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

            COPY INTO OPEN_PAYMENTS_GENERAL (DATA, _SOURCE_FILE)
            FROM (
                SELECT $1, METADATA$FILENAME
                FROM @BRONZE_S3_STAGE/open_payments/
            )
            FILE_FORMAT = (TYPE = PARQUET)
            PATTERN = '.*General.*LATEST.*parquet'
            ON_ERROR = 'CONTINUE';
        """,
        statement_count=3,
    )

    # -------------------------------------------------------------------
    # TASK 6 : Load Open Payments Research
    # -------------------------------------------------------------------
    load_open_payments_research = SnowflakeSqlApiOperator(
        task_id='load_open_payments_research',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

            COPY INTO OPEN_PAYMENTS_RESEARCH (DATA, _SOURCE_FILE)
            FROM (
                SELECT $1, METADATA$FILENAME
                FROM @BRONZE_S3_STAGE/open_payments/
            )
            FILE_FORMAT = (TYPE = PARQUET)
            PATTERN = '.*Research.*LATEST.*parquet'
            ON_ERROR = 'CONTINUE';
        """,
        statement_count=3,
    )

    # -------------------------------------------------------------------
    # TASK 7 : Load Open Payments Ownership
    # -------------------------------------------------------------------
    load_open_payments_ownership = SnowflakeSqlApiOperator(
        task_id='load_open_payments_ownership',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

            COPY INTO OPEN_PAYMENTS_OWNERSHIP (DATA, _SOURCE_FILE)
            FROM (
                SELECT $1, METADATA$FILENAME
                FROM @BRONZE_S3_STAGE/open_payments/
            )
            FILE_FORMAT = (TYPE = PARQUET)
            PATTERN = '.*Ownership.*LATEST.*parquet'
            ON_ERROR = 'CONTINUE';
        """,
        statement_count=3,
    )

    # -------------------------------------------------------------------
    # TASK 8 : Load Provider Information
    # -------------------------------------------------------------------
    load_provider_info = SnowflakeSqlApiOperator(
        task_id='load_provider_information',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

            COPY INTO PROVIDER_INFORMATION (
                FEDERAL_PROVIDER_NUMBER, PROVIDER_NAME, PROVIDER_ADDRESS, PROVIDER_CITY,
                PROVIDER_STATE, PROVIDER_ZIP_CODE, PROVIDER_PHONE_NUMBER,
                PROVIDER_SSA_COUNTY_CODE, PROVIDER_COUNTY_NAME, OWNERSHIP_TYPE,
                NUMBER_OF_CERTIFIED_BEDS, NUMBER_OF_RESIDENTS, AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY,
                PROVIDER_TYPE, PROVIDER_RESIDES_IN_HOSPITAL, LEGAL_BUSINESS_NAME,
                DATE_FIRST_APPROVED, CONTINUING_CARE_RETIREMENT_COMMUNITY, SPECIAL_FOCUS_FACILITY,
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
        """,
        statement_count=3,
    )

    # -------------------------------------------------------------------
    # TASK 9 : Load Long-Term Care Hospital
    # -------------------------------------------------------------------
    load_ltch = SnowflakeSqlApiOperator(
        task_id='load_longterm_care_hospital',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

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
        """,
        statement_count=3,
    )

    # -------------------------------------------------------------------
    # TASK 10 : Load Hospice
    # -------------------------------------------------------------------
    load_hospice = SnowflakeSqlApiOperator(
        task_id='load_hospice',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

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
        """,
        statement_count=3,
    )

    # -------------------------------------------------------------------
    # TASK 11 : Load Home Health Care
    # -------------------------------------------------------------------
    load_home_health = SnowflakeSqlApiOperator(
        task_id='load_home_health_care',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

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
        """,
        statement_count=3,
    )

    # -------------------------------------------------------------------
    # TASK 12 : Load Medicare Part D Prescribers
    # -------------------------------------------------------------------
    load_part_d = SnowflakeSqlApiOperator(
        task_id='load_medicare_part_d_prescribers',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE AI_FACTORY_DB;
            USE SCHEMA BRONZE;

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
                    $1:Prscrbr_NPI::VARCHAR, $1:Prscrbr_Last_Org_Name::VARCHAR,
                    $1:Prscrbr_First_Name::VARCHAR, $1:Prscrbr_City::VARCHAR,
                    $1:Prscrbr_State_Abrvtn::VARCHAR, $1:Prscrbr_State_FIPS::VARCHAR,
                    $1:Prscrbr_Type::VARCHAR, $1:Prscrbr_Type_Src::VARCHAR,
                    $1:Brnd_Name::VARCHAR, $1:Gnrc_Name::VARCHAR,
                    $1:Tot_Clms::VARCHAR, $1:Tot_30day_Fills::VARCHAR,
                    $1:Tot_Day_Suply::VARCHAR, $1:Tot_Drug_Cst::VARCHAR, $1:Tot_Benes::VARCHAR,
                    $1:GE65_Sprsn_Flag::VARCHAR, $1:GE65_Tot_Clms::VARCHAR,
                    $1:GE65_Tot_30day_Fills::VARCHAR, $1:GE65_Tot_Day_Suply::VARCHAR,
                    $1:GE65_Tot_Drug_Cst::VARCHAR, $1:GE65_Bene_Sprsn_Flag::VARCHAR,
                    $1:GE65_Tot_Benes::VARCHAR, METADATA$FILENAME
                FROM @BRONZE_S3_STAGE/medicare_part_d_prescribers/
            )
            FILE_FORMAT = (TYPE = PARQUET)
            PATTERN = '.*LATEST.*parquet'
            ON_ERROR = 'CONTINUE';
        """,
        statement_count=3,
    )

    # -------------------------------------------------------------------
    # TASK 13 : Summary
    # -------------------------------------------------------------------
    def summarize_load(**context):
        logger.info("="*70)
        logger.info("LOAD BRONZE TABLES - SUMMARY")
        logger.info("="*70)
        logger.info("Tables chargees dans AI_FACTORY_DB.BRONZE:")
        logger.info("  - LEIE (Excluded Individuals/Entities)")
        logger.info("  - MEDICARE_HOSPITAL_SPENDING")
        logger.info("  - OPEN_PAYMENTS_GENERAL")
        logger.info("  - OPEN_PAYMENTS_RESEARCH")
        logger.info("  - OPEN_PAYMENTS_OWNERSHIP")
        logger.info("  - PROVIDER_INFORMATION")
        logger.info("  - LONGTERM_CARE_HOSPITAL")
        logger.info("  - HOSPICE")
        logger.info("  - HOME_HEALTH_CARE")
        logger.info("  - MEDICARE_PART_D_PRESCRIBERS")
        logger.info("="*70)
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info("="*70)

    summary_task = PythonOperator(
        task_id='summary',
        python_callable=summarize_load,
    )

    # -------------------------------------------------------------------
    # DAG DEPENDENCIES
    # Tables -> Truncate -> Load en parallele -> Summary
    # -------------------------------------------------------------------
    create_tables >> truncate_tables

    # Load tasks en parallele apres truncate
    truncate_tables >> [
        load_leie,
        load_medicare_spending,
        load_open_payments_general,
        load_open_payments_research,
        load_open_payments_ownership,
        load_provider_info,
        load_ltch,
        load_hospice,
        load_home_health,
        load_part_d
    ] >> summary_task
