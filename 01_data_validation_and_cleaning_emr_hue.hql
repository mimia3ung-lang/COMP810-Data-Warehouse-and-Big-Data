-- =====================================================================
-- 01_data_validation_and_cleaning_emr_hue.hql
-- Environment: Amazon EMR + Hue + Hive + Amazon S3
-- Project: Big Data Analysis of Customer Churn Patterns
--
-- BEFORE RUNNING:
-- 1. Replace every REPLACE_BUCKET_NAME with your S3 bucket.
-- 2. Upload the raw CSV to:
--      s3://REPLACE_BUCKET_NAME/customer-churn/raw/
-- 3. Keep raw, typed, cleaned, and export prefixes separate.
-- 4. Run the script section by section in Hue for easier troubleshooting.
-- 5. The signup_date conversion assumes yyyy-MM-dd.
--
-- S3 locations used:
-- raw:     s3://REPLACE_BUCKET_NAME/customer-churn/raw/
-- typed:   s3://REPLACE_BUCKET_NAME/customer-churn/typed_orc/
-- cleaned: s3://REPLACE_BUCKET_NAME/customer-churn/cleaned_orc/
-- export:  s3://REPLACE_BUCKET_NAME/customer-churn/export_tsv/
-- =====================================================================

SET hive.execution.engine=tez;
SET hive.exec.compress.output=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE DATABASE IF NOT EXISTS customer_churn_db;
USE customer_churn_db;

-- =====================================================================
-- SECTION 1: RAW EXTERNAL CSV TABLE
-- All raw fields are STRING so malformed values can be identified safely.
-- =====================================================================

DROP TABLE IF EXISTS customer_churn_raw;

CREATE EXTERNAL TABLE customer_churn_raw (
    customer_id                  STRING,
    signup_date                  STRING,
    age                          STRING,
    gender                       STRING,
    annual_income                STRING,
    education                    STRING,
    marital_status               STRING,
    dependents                   STRING,
    tenure                       STRING,
    contract                     STRING,
    payment_method               STRING,
    paperless_billing            STRING,
    senior_citizen               STRING,
    monthlycharges               STRING,
    totalcharges                 STRING,
    num_services                 STRING,
    has_phone_service            STRING,
    has_internet_service         STRING,
    has_online_security          STRING,
    has_online_backup            STRING,
    has_device_protection        STRING,
    has_tech_support             STRING,
    has_streaming_tv             STRING,
    has_streaming_movies         STRING,
    customer_satisfaction        STRING,
    num_complaints               STRING,
    num_service_calls            STRING,
    late_payments                STRING,
    avg_monthly_gb               STRING,
    days_since_last_interaction  STRING,
    credit_score                 STRING,
    churn                        STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 's3://REPLACE_BUCKET_NAME/customer-churn/raw/'
TBLPROPERTIES ("skip.header.line.count" = "1");

-- Confirm table and source data.
DESCRIBE FORMATTED customer_churn_raw;

SELECT COUNT(*) AS raw_record_count
FROM customer_churn_raw;

SELECT *
FROM customer_churn_raw
LIMIT 10;


-- =====================================================================
-- SECTION 2: INITIAL DATA-QUALITY PROFILE
-- =====================================================================

-- 2.1 Customer-ID uniqueness.
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT TRIM(customer_id)) AS distinct_customer_ids,
    COUNT(*) - COUNT(DISTINCT TRIM(customer_id)) AS duplicate_customer_id_rows
FROM customer_churn_raw;

-- 2.2 Duplicate customer IDs.
SELECT
    TRIM(customer_id) AS customer_id,
    COUNT(*) AS occurrence_count
FROM customer_churn_raw
WHERE customer_id IS NOT NULL
  AND TRIM(customer_id) <> ''
GROUP BY TRIM(customer_id)
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 100;

-- 2.3 Missing values in the key analytical fields.
SELECT 'customer_id' AS field_name,
       SUM(CASE WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 1 ELSE 0 END) AS missing_count
FROM customer_churn_raw
UNION ALL
SELECT 'signup_date',
       SUM(CASE WHEN signup_date IS NULL OR TRIM(signup_date) = '' THEN 1 ELSE 0 END)
FROM customer_churn_raw
UNION ALL
SELECT 'tenure',
       SUM(CASE WHEN tenure IS NULL OR TRIM(tenure) = '' THEN 1 ELSE 0 END)
FROM customer_churn_raw
UNION ALL
SELECT 'contract',
       SUM(CASE WHEN contract IS NULL OR TRIM(contract) = '' THEN 1 ELSE 0 END)
FROM customer_churn_raw
UNION ALL
SELECT 'payment_method',
       SUM(CASE WHEN payment_method IS NULL OR TRIM(payment_method) = '' THEN 1 ELSE 0 END)
FROM customer_churn_raw
UNION ALL
SELECT 'monthlycharges',
       SUM(CASE WHEN monthlycharges IS NULL OR TRIM(monthlycharges) = '' THEN 1 ELSE 0 END)
FROM customer_churn_raw
UNION ALL
SELECT 'totalcharges',
       SUM(CASE WHEN totalcharges IS NULL OR TRIM(totalcharges) = '' THEN 1 ELSE 0 END)
FROM customer_churn_raw
UNION ALL
SELECT 'customer_satisfaction',
       SUM(CASE WHEN customer_satisfaction IS NULL OR TRIM(customer_satisfaction) = '' THEN 1 ELSE 0 END)
FROM customer_churn_raw
UNION ALL
SELECT 'num_complaints',
       SUM(CASE WHEN num_complaints IS NULL OR TRIM(num_complaints) = '' THEN 1 ELSE 0 END)
FROM customer_churn_raw
UNION ALL
SELECT 'avg_monthly_gb',
       SUM(CASE WHEN avg_monthly_gb IS NULL OR TRIM(avg_monthly_gb) = '' THEN 1 ELSE 0 END)
FROM customer_churn_raw
UNION ALL
SELECT 'churn',
       SUM(CASE WHEN churn IS NULL OR TRIM(churn) = '' THEN 1 ELSE 0 END)
FROM customer_churn_raw;

-- 2.4 Raw target values.
SELECT
    LOWER(TRIM(churn)) AS raw_churn_value,
    COUNT(*) AS record_count
FROM customer_churn_raw
GROUP BY LOWER(TRIM(churn))
ORDER BY record_count DESC;

-- 2.5 Main category values.
SELECT
    LOWER(TRIM(contract)) AS contract,
    COUNT(*) AS record_count
FROM customer_churn_raw
GROUP BY LOWER(TRIM(contract))
ORDER BY record_count DESC;

SELECT
    LOWER(TRIM(payment_method)) AS payment_method,
    COUNT(*) AS record_count
FROM customer_churn_raw
GROUP BY LOWER(TRIM(payment_method))
ORDER BY record_count DESC;


-- =====================================================================
-- SECTION 3: TYPED EXTERNAL ORC TABLE
-- Invalid conversions become NULL and are reviewed before final cleaning.
-- =====================================================================

DROP TABLE IF EXISTS customer_churn_typed;

CREATE EXTERNAL TABLE customer_churn_typed (
    customer_id                  STRING,
    signup_date                  DATE,
    age                          INT,
    gender                       STRING,
    annual_income                DOUBLE,
    education                    STRING,
    marital_status               STRING,
    dependents                   INT,
    tenure                       INT,
    contract                     STRING,
    payment_method               STRING,
    paperless_billing            INT,
    senior_citizen               INT,
    monthlycharges               DOUBLE,
    totalcharges                 DOUBLE,
    num_services                 INT,
    has_phone_service            INT,
    has_internet_service         INT,
    has_online_security          INT,
    has_online_backup            INT,
    has_device_protection        INT,
    has_tech_support             INT,
    has_streaming_tv             INT,
    has_streaming_movies         INT,
    customer_satisfaction        INT,
    num_complaints               INT,
    num_service_calls            INT,
    late_payments                INT,
    avg_monthly_gb               DOUBLE,
    days_since_last_interaction  INT,
    credit_score                 INT,
    churn                        INT
)
STORED AS ORC
LOCATION 's3://REPLACE_BUCKET_NAME/customer-churn/typed_orc/';

INSERT OVERWRITE TABLE customer_churn_typed
SELECT
    CASE
        WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN NULL
        ELSE TRIM(customer_id)
    END AS customer_id,

    CASE
        WHEN TRIM(signup_date) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        THEN CAST(TRIM(signup_date) AS DATE)
        ELSE NULL
    END AS signup_date,

    CASE
        WHEN CAST(TRIM(age) AS INT) BETWEEN 0 AND 120
        THEN CAST(TRIM(age) AS INT)
        ELSE NULL
    END AS age,

    CASE
        WHEN gender IS NULL OR TRIM(gender) = '' THEN NULL
        ELSE LOWER(TRIM(gender))
    END AS gender,

    CASE
        WHEN CAST(TRIM(annual_income) AS DOUBLE) >= 0
        THEN CAST(TRIM(annual_income) AS DOUBLE)
        ELSE NULL
    END AS annual_income,

    CASE
        WHEN education IS NULL OR TRIM(education) = '' THEN NULL
        ELSE LOWER(TRIM(education))
    END AS education,

    CASE
        WHEN marital_status IS NULL OR TRIM(marital_status) = '' THEN NULL
        ELSE LOWER(TRIM(marital_status))
    END AS marital_status,

    CASE
        WHEN CAST(TRIM(dependents) AS INT) >= 0
        THEN CAST(TRIM(dependents) AS INT)
        ELSE NULL
    END AS dependents,

    CASE
        WHEN CAST(TRIM(tenure) AS INT) >= 0
        THEN CAST(TRIM(tenure) AS INT)
        ELSE NULL
    END AS tenure,

    CASE
        WHEN contract IS NULL OR TRIM(contract) = '' THEN NULL
        ELSE LOWER(TRIM(contract))
    END AS contract,

    CASE
        WHEN payment_method IS NULL OR TRIM(payment_method) = '' THEN NULL
        ELSE LOWER(TRIM(payment_method))
    END AS payment_method,

    CASE
        WHEN LOWER(TRIM(paperless_billing)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(paperless_billing)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS paperless_billing,

    CASE
        WHEN LOWER(TRIM(senior_citizen)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(senior_citizen)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS senior_citizen,

    CASE
        WHEN CAST(TRIM(monthlycharges) AS DOUBLE) >= 0
        THEN CAST(TRIM(monthlycharges) AS DOUBLE)
        ELSE NULL
    END AS monthlycharges,

    CASE
        WHEN CAST(TRIM(totalcharges) AS DOUBLE) >= 0
        THEN CAST(TRIM(totalcharges) AS DOUBLE)
        ELSE NULL
    END AS totalcharges,

    CASE
        WHEN CAST(TRIM(num_services) AS INT) >= 0
        THEN CAST(TRIM(num_services) AS INT)
        ELSE NULL
    END AS num_services,

    CASE
        WHEN LOWER(TRIM(has_phone_service)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(has_phone_service)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS has_phone_service,

    CASE
        WHEN LOWER(TRIM(has_internet_service)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(has_internet_service)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS has_internet_service,

    CASE
        WHEN LOWER(TRIM(has_online_security)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(has_online_security)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS has_online_security,

    CASE
        WHEN LOWER(TRIM(has_online_backup)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(has_online_backup)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS has_online_backup,

    CASE
        WHEN LOWER(TRIM(has_device_protection)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(has_device_protection)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS has_device_protection,

    CASE
        WHEN LOWER(TRIM(has_tech_support)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(has_tech_support)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS has_tech_support,

    CASE
        WHEN LOWER(TRIM(has_streaming_tv)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(has_streaming_tv)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS has_streaming_tv,

    CASE
        WHEN LOWER(TRIM(has_streaming_movies)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(has_streaming_movies)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS has_streaming_movies,

    CASE
        WHEN CAST(TRIM(customer_satisfaction) AS INT) BETWEEN 1 AND 5
        THEN CAST(TRIM(customer_satisfaction) AS INT)
        ELSE NULL
    END AS customer_satisfaction,

    CASE
        WHEN CAST(TRIM(num_complaints) AS INT) >= 0
        THEN CAST(TRIM(num_complaints) AS INT)
        ELSE NULL
    END AS num_complaints,

    CASE
        WHEN CAST(TRIM(num_service_calls) AS INT) >= 0
        THEN CAST(TRIM(num_service_calls) AS INT)
        ELSE NULL
    END AS num_service_calls,

    CASE
        WHEN CAST(TRIM(late_payments) AS INT) >= 0
        THEN CAST(TRIM(late_payments) AS INT)
        ELSE NULL
    END AS late_payments,

    CASE
        WHEN CAST(TRIM(avg_monthly_gb) AS DOUBLE) >= 0
        THEN CAST(TRIM(avg_monthly_gb) AS DOUBLE)
        ELSE NULL
    END AS avg_monthly_gb,

    CASE
        WHEN CAST(TRIM(days_since_last_interaction) AS INT) >= 0
        THEN CAST(TRIM(days_since_last_interaction) AS INT)
        ELSE NULL
    END AS days_since_last_interaction,

    CASE
        WHEN CAST(TRIM(credit_score) AS INT) BETWEEN 0 AND 1000
        THEN CAST(TRIM(credit_score) AS INT)
        ELSE NULL
    END AS credit_score,

    CASE
        WHEN LOWER(TRIM(churn)) IN ('1','yes','y','true') THEN 1
        WHEN LOWER(TRIM(churn)) IN ('0','no','n','false') THEN 0
        ELSE NULL
    END AS churn
FROM customer_churn_raw;


-- =====================================================================
-- SECTION 4: POST-CONVERSION VALIDATION
-- =====================================================================

SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS missing_customer_id,
    SUM(CASE WHEN signup_date IS NULL THEN 1 ELSE 0 END) AS invalid_or_missing_signup_date,
    SUM(CASE WHEN churn IS NULL THEN 1 ELSE 0 END) AS invalid_or_missing_churn
FROM customer_churn_typed;

SELECT
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS invalid_or_missing_age,
    SUM(CASE WHEN annual_income IS NULL THEN 1 ELSE 0 END) AS invalid_or_missing_income,
    SUM(CASE WHEN tenure IS NULL THEN 1 ELSE 0 END) AS invalid_or_missing_tenure,
    SUM(CASE WHEN monthlycharges IS NULL THEN 1 ELSE 0 END) AS invalid_or_missing_monthlycharges,
    SUM(CASE WHEN totalcharges IS NULL THEN 1 ELSE 0 END) AS invalid_or_missing_totalcharges,
    SUM(CASE WHEN customer_satisfaction IS NULL THEN 1 ELSE 0 END) AS invalid_or_missing_satisfaction,
    SUM(CASE WHEN avg_monthly_gb IS NULL THEN 1 ELSE 0 END) AS invalid_or_missing_usage
FROM customer_churn_typed;

-- Inspect invalid target rows before they are excluded.
SELECT *
FROM customer_churn_typed
WHERE customer_id IS NULL
   OR churn IS NULL
LIMIT 100;


-- =====================================================================
-- SECTION 5: CLEANED EXTERNAL ORC TABLE
-- Eight derived analytical groups are added.
-- =====================================================================

DROP TABLE IF EXISTS customer_churn_cleaned;

CREATE EXTERNAL TABLE customer_churn_cleaned (
    customer_id                  STRING,
    signup_date                  DATE,
    age                          INT,
    gender                       STRING,
    annual_income                DOUBLE,
    education                    STRING,
    marital_status               STRING,
    dependents                   INT,
    tenure                       INT,
    contract                     STRING,
    payment_method               STRING,
    paperless_billing            INT,
    senior_citizen               INT,
    monthlycharges               DOUBLE,
    totalcharges                 DOUBLE,
    num_services                 INT,
    has_phone_service            INT,
    has_internet_service         INT,
    has_online_security          INT,
    has_online_backup            INT,
    has_device_protection        INT,
    has_tech_support             INT,
    has_streaming_tv             INT,
    has_streaming_movies         INT,
    customer_satisfaction        INT,
    num_complaints               INT,
    num_service_calls            INT,
    late_payments                INT,
    avg_monthly_gb               DOUBLE,
    days_since_last_interaction  INT,
    credit_score                 INT,
    churn                        INT,
    tenure_group                 STRING,
    satisfaction_group           STRING,
    complaint_group              STRING,
    service_call_group           STRING,
    late_payment_group           STRING,
    interaction_recency_group    STRING,
    monthly_charge_band          STRING,
    usage_band                   STRING
)
STORED AS ORC
LOCATION 's3://REPLACE_BUCKET_NAME/customer-churn/cleaned_orc/';

WITH ranked AS (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY signup_date DESC
        ) AS row_rank
    FROM customer_churn_typed t
    WHERE customer_id IS NOT NULL
      AND churn IN (0, 1)
),
deduplicated AS (
    SELECT
        customer_id,
        signup_date,
        age,
        gender,
        annual_income,
        education,
        marital_status,
        dependents,
        tenure,
        contract,
        payment_method,
        paperless_billing,
        senior_citizen,
        monthlycharges,
        totalcharges,
        num_services,
        has_phone_service,
        has_internet_service,
        has_online_security,
        has_online_backup,
        has_device_protection,
        has_tech_support,
        has_streaming_tv,
        has_streaming_movies,
        customer_satisfaction,
        num_complaints,
        num_service_calls,
        late_payments,
        avg_monthly_gb,
        days_since_last_interaction,
        credit_score,
        churn
    FROM ranked
    WHERE row_rank = 1
),
thresholds AS (
    SELECT
        percentile_approx(
            monthlycharges,
            array(0.3333D, 0.6667D)
        ) AS charge_percentiles,
        percentile_approx(
            CASE
                WHEN has_internet_service = 1 THEN avg_monthly_gb
                ELSE NULL
            END,
            array(0.3333D, 0.6667D)
        ) AS usage_percentiles
    FROM deduplicated
)
INSERT OVERWRITE TABLE customer_churn_cleaned
SELECT
    d.customer_id,
    d.signup_date,
    d.age,
    d.gender,
    d.annual_income,
    d.education,
    d.marital_status,
    d.dependents,
    d.tenure,
    d.contract,
    d.payment_method,
    d.paperless_billing,
    d.senior_citizen,
    d.monthlycharges,
    d.totalcharges,
    d.num_services,
    d.has_phone_service,
    d.has_internet_service,
    d.has_online_security,
    d.has_online_backup,
    d.has_device_protection,
    d.has_tech_support,
    d.has_streaming_tv,
    d.has_streaming_movies,
    d.customer_satisfaction,
    d.num_complaints,
    d.num_service_calls,
    d.late_payments,

    CASE
        WHEN d.has_internet_service = 0 AND d.avg_monthly_gb IS NULL THEN 0.0
        ELSE d.avg_monthly_gb
    END AS avg_monthly_gb,

    d.days_since_last_interaction,
    d.credit_score,
    d.churn,

    CASE
        WHEN d.tenure IS NULL THEN 'missing'
        WHEN d.tenure BETWEEN 0 AND 12 THEN '0-12_months'
        WHEN d.tenure BETWEEN 13 AND 24 THEN '13-24_months'
        WHEN d.tenure BETWEEN 25 AND 48 THEN '25-48_months'
        ELSE '49+_months'
    END AS tenure_group,

    CASE
        WHEN d.customer_satisfaction IS NULL THEN 'missing'
        WHEN d.customer_satisfaction BETWEEN 1 AND 2 THEN 'low'
        WHEN d.customer_satisfaction = 3 THEN 'moderate'
        ELSE 'high'
    END AS satisfaction_group,

    CASE
        WHEN d.num_complaints IS NULL THEN 'missing'
        WHEN d.num_complaints = 0 THEN 'none'
        WHEN d.num_complaints = 1 THEN 'one'
        WHEN d.num_complaints BETWEEN 2 AND 3 THEN 'two_to_three'
        ELSE 'more_than_three'
    END AS complaint_group,

    CASE
        WHEN d.num_service_calls IS NULL THEN 'missing'
        WHEN d.num_service_calls = 0 THEN 'none'
        WHEN d.num_service_calls BETWEEN 1 AND 2 THEN 'one_to_two'
        WHEN d.num_service_calls BETWEEN 3 AND 5 THEN 'three_to_five'
        ELSE 'more_than_five'
    END AS service_call_group,

    CASE
        WHEN d.late_payments IS NULL THEN 'missing'
        WHEN d.late_payments = 0 THEN 'none'
        WHEN d.late_payments BETWEEN 1 AND 2 THEN 'one_to_two'
        WHEN d.late_payments BETWEEN 3 AND 5 THEN 'three_to_five'
        ELSE 'more_than_five'
    END AS late_payment_group,

    CASE
        WHEN d.days_since_last_interaction IS NULL THEN 'missing'
        WHEN d.days_since_last_interaction BETWEEN 0 AND 30 THEN '0-30_days'
        WHEN d.days_since_last_interaction BETWEEN 31 AND 60 THEN '31-60_days'
        WHEN d.days_since_last_interaction BETWEEN 61 AND 90 THEN '61-90_days'
        ELSE '91+_days'
    END AS interaction_recency_group,

    CASE
        WHEN d.monthlycharges IS NULL THEN 'missing'
        WHEN d.monthlycharges <= th.charge_percentiles[0] THEN 'low'
        WHEN d.monthlycharges <= th.charge_percentiles[1] THEN 'medium'
        ELSE 'high'
    END AS monthly_charge_band,

    CASE
        WHEN d.has_internet_service = 0 THEN 'no_internet'
        WHEN d.avg_monthly_gb IS NULL THEN 'missing'
        WHEN d.avg_monthly_gb <= th.usage_percentiles[0] THEN 'low'
        WHEN d.avg_monthly_gb <= th.usage_percentiles[1] THEN 'medium'
        ELSE 'high'
    END AS usage_band
FROM deduplicated d
CROSS JOIN thresholds th;


-- =====================================================================
-- SECTION 6: FINAL QUALITY CHECKS AND FUNDAMENTAL STATISTICS
-- =====================================================================

-- 6.1 Cleaned record count and uniqueness.
SELECT
    COUNT(*) AS cleaned_rows,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(*) - COUNT(DISTINCT customer_id) AS remaining_duplicate_rows,
    SUM(CASE WHEN churn IS NULL THEN 1 ELSE 0 END) AS missing_churn
FROM customer_churn_cleaned;

-- 6.2 Overall churn summary.
SELECT
    CASE WHEN churn = 1 THEN 'Churned' ELSE 'Retained' END AS customer_status,
    COUNT(*) AS customer_count,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        4
    ) AS percentage
FROM customer_churn_cleaned
GROUP BY churn
ORDER BY churn;

-- 6.3 Descriptive statistics.
SELECT
    COUNT(*) AS record_count,

    ROUND(AVG(tenure), 2) AS mean_tenure,
    percentile_approx(CAST(tenure AS DOUBLE), 0.5) AS median_tenure,
    MIN(tenure) AS minimum_tenure,
    MAX(tenure) AS maximum_tenure,
    ROUND(stddev_pop(tenure), 2) AS stddev_tenure,

    ROUND(AVG(monthlycharges), 2) AS mean_monthlycharges,
    percentile_approx(monthlycharges, 0.5) AS median_monthlycharges,
    MIN(monthlycharges) AS minimum_monthlycharges,
    MAX(monthlycharges) AS maximum_monthlycharges,
    ROUND(stddev_pop(monthlycharges), 2) AS stddev_monthlycharges,

    ROUND(AVG(totalcharges), 2) AS mean_totalcharges,
    percentile_approx(totalcharges, 0.5) AS median_totalcharges,
    MIN(totalcharges) AS minimum_totalcharges,
    MAX(totalcharges) AS maximum_totalcharges,

    ROUND(AVG(customer_satisfaction), 2) AS mean_satisfaction,
    percentile_approx(
        CAST(customer_satisfaction AS DOUBLE),
        0.5
    ) AS median_satisfaction

FROM customer_churn_cleaned;

-- 6.4 Category frequencies.
SELECT contract, COUNT(*) AS customer_count
FROM customer_churn_cleaned
GROUP BY contract
ORDER BY customer_count DESC;

SELECT payment_method, COUNT(*) AS customer_count
FROM customer_churn_cleaned
GROUP BY payment_method
ORDER BY customer_count DESC;

SELECT tenure_group, COUNT(*) AS customer_count
FROM customer_churn_cleaned
GROUP BY tenure_group
ORDER BY customer_count DESC;

-- 6.5 Missing values after cleaning.
SELECT
    SUM(CASE WHEN tenure IS NULL THEN 1 ELSE 0 END) AS missing_tenure,
    SUM(CASE WHEN contract IS NULL THEN 1 ELSE 0 END) AS missing_contract,
    SUM(CASE WHEN payment_method IS NULL THEN 1 ELSE 0 END) AS missing_payment_method,
    SUM(CASE WHEN monthlycharges IS NULL THEN 1 ELSE 0 END) AS missing_monthlycharges,
    SUM(CASE WHEN totalcharges IS NULL THEN 1 ELSE 0 END) AS missing_totalcharges,
    SUM(CASE WHEN customer_satisfaction IS NULL THEN 1 ELSE 0 END) AS missing_satisfaction,
    SUM(CASE WHEN num_complaints IS NULL THEN 1 ELSE 0 END) AS missing_complaints,
    SUM(CASE WHEN avg_monthly_gb IS NULL THEN 1 ELSE 0 END) AS missing_usage
FROM customer_churn_cleaned;


-- =====================================================================
-- SECTION 7: CHURN AGGREGATION FOR MAPREDUCE/RESULT VALIDATION
-- =====================================================================

SELECT
    contract,
    payment_method,
    COUNT(*) AS total_customers,
    SUM(churn) AS total_churned,
    ROUND(SUM(churn) * 100.0 / COUNT(*), 4) AS churn_rate
FROM customer_churn_cleaned
GROUP BY contract, payment_method
ORDER BY churn_rate DESC;


-- =====================================================================
-- SECTION 8: OPTIONAL TSV EXPORT FOR ELASTICSEARCH
--
-- Hive writes one or more part files to this S3 prefix.
-- The export has no header. Use the cleaned ORC table directly when
-- possible, or add a header during the Elasticsearch ingestion process.
--
-- IMPORTANT: Use a fresh/empty export prefix when rerunning.
-- =====================================================================

INSERT OVERWRITE DIRECTORY
    's3://REPLACE_BUCKET_NAME/customer-churn/export_tsv/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    customer_id,
    CAST(signup_date AS STRING),
    age,
    gender,
    annual_income,
    education,
    marital_status,
    dependents,
    tenure,
    contract,
    payment_method,
    paperless_billing,
    senior_citizen,
    monthlycharges,
    totalcharges,
    num_services,
    has_phone_service,
    has_internet_service,
    has_online_security,
    has_online_backup,
    has_device_protection,
    has_tech_support,
    has_streaming_tv,
    has_streaming_movies,
    customer_satisfaction,
    num_complaints,
    num_service_calls,
    late_payments,
    avg_monthly_gb,
    days_since_last_interaction,
    credit_score,
    churn,
    tenure_group,
    satisfaction_group,
    complaint_group,
    service_call_group,
    late_payment_group,
    interaction_recency_group,
    monthly_charge_band,
    usage_band
FROM customer_churn_cleaned;

-- End of script.
