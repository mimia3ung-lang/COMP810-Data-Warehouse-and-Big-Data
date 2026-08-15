-- ============================================================
-- Customer Churn Big Data Project
-- Final Hive Visualisation Queries
-- ============================================================
-- Source table: customer_churn_cleaned
-- Purpose: Generate the aggregated outputs used for the final
--          Hue visualisations in the customer churn report.
-- ============================================================

-- 1. Overall Churn Distribution
SELECT
    CASE
        WHEN churn = 1 THEN 'Churned'
        ELSE 'Retained'
    END AS customer_status,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM customer_churn_cleaned
GROUP BY churn
ORDER BY churn;

-- 2. Contract x Payment Method
SELECT
    contract AS contract_type,
    payment_method,
    COUNT(*) AS total_customers,
    SUM(churn) AS churned_customers,
    ROUND(SUM(churn) * 100.0 / COUNT(*), 2) AS churn_rate
FROM customer_churn_cleaned
WHERE contract IS NOT NULL
  AND payment_method IS NOT NULL
GROUP BY contract, payment_method
ORDER BY contract_type, churn_rate DESC;

-- 3. Tenure x Satisfaction
SELECT
    CASE tenure_group
        WHEN '0-12_months' THEN '0-12 months'
        WHEN '13-24_months' THEN '13-24 months'
        WHEN '25-48_months' THEN '25-48 months'
        WHEN '49+_months' THEN '49+ months'
    END AS tenure,
    CASE satisfaction_group
        WHEN 'low' THEN 'Low (1-2)'
        WHEN 'moderate' THEN 'Moderate (3)'
        WHEN 'high' THEN 'High (4-5)'
    END AS satisfaction,
    COUNT(*) AS total_customers,
    SUM(churn) AS churned_customers,
    ROUND(SUM(churn) * 100.0 / COUNT(*), 2) AS churn_rate
FROM customer_churn_cleaned
WHERE tenure_group <> 'missing'
  AND satisfaction_group <> 'missing'
GROUP BY tenure_group, satisfaction_group
ORDER BY
    CASE tenure_group
        WHEN '0-12_months' THEN 1
        WHEN '13-24_months' THEN 2
        WHEN '25-48_months' THEN 3
        WHEN '49+_months' THEN 4
        ELSE 5
    END,
    CASE satisfaction_group
        WHEN 'low' THEN 1
        WHEN 'moderate' THEN 2
        WHEN 'high' THEN 3
        ELSE 4
    END;

-- 4. Complaints x Service Calls
SELECT
    CASE complaint_group
        WHEN 'none' THEN 'No complaints'
        WHEN 'one' THEN '1 complaint'
        WHEN 'two_to_three' THEN '2-3 complaints'
        WHEN 'more_than_three' THEN 'More than 3'
    END AS complaints,
    CASE service_call_group
        WHEN 'none' THEN 'No calls'
        WHEN 'one_to_two' THEN '1-2 calls'
        WHEN 'three_to_five' THEN '3-5 calls'
        WHEN 'more_than_five' THEN 'More than 5'
    END AS service_calls,
    COUNT(*) AS total_customers,
    SUM(churn) AS churned_customers,
    ROUND(SUM(churn) * 100.0 / COUNT(*), 2) AS churn_rate
FROM customer_churn_cleaned
WHERE complaint_group <> 'missing'
  AND service_call_group <> 'missing'
GROUP BY complaint_group, service_call_group
ORDER BY
    CASE complaint_group
        WHEN 'none' THEN 1
        WHEN 'one' THEN 2
        WHEN 'two_to_three' THEN 3
        WHEN 'more_than_three' THEN 4
        ELSE 5
    END,
    CASE service_call_group
        WHEN 'none' THEN 1
        WHEN 'one_to_two' THEN 2
        WHEN 'three_to_five' THEN 3
        WHEN 'more_than_five' THEN 4
        ELSE 5
    END;

-- 5. Internet Usage x Tech Support
SELECT
    CASE usage_band
        WHEN 'no_internet' THEN 'No internet'
        WHEN 'low' THEN 'Low usage'
        WHEN 'medium' THEN 'Medium usage'
        WHEN 'high' THEN 'High usage'
    END AS internet_usage,
    CASE
        WHEN has_tech_support = 1 THEN 'Has Tech Support'
        WHEN has_tech_support = 0 THEN 'No Tech Support'
    END AS tech_support,
    COUNT(*) AS total_customers,
    SUM(churn) AS churned_customers,
    ROUND(SUM(churn) * 100.0 / COUNT(*), 2) AS churn_rate
FROM customer_churn_cleaned
WHERE usage_band <> 'missing'
  AND has_tech_support IN (0, 1)
GROUP BY usage_band, has_tech_support
ORDER BY
    CASE usage_band
        WHEN 'no_internet' THEN 1
        WHEN 'low' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'high' THEN 4
        ELSE 5
    END,
    has_tech_support DESC;

-- 6. Monthly Charges x Contract
SELECT
    CASE monthly_charge_band
        WHEN 'low' THEN 'Low charges'
        WHEN 'medium' THEN 'Medium charges'
        WHEN 'high' THEN 'High charges'
    END AS charge_band,
    contract AS contract_type,
    COUNT(*) AS total_customers,
    SUM(churn) AS churned_customers,
    ROUND(SUM(churn) * 100.0 / COUNT(*), 2) AS churn_rate
FROM customer_churn_cleaned
WHERE monthly_charge_band <> 'missing'
  AND contract IS NOT NULL
GROUP BY monthly_charge_band, contract
ORDER BY
    CASE monthly_charge_band
        WHEN 'low' THEN 1
        WHEN 'medium' THEN 2
        WHEN 'high' THEN 3
        ELSE 4
    END,
    churn_rate DESC;
