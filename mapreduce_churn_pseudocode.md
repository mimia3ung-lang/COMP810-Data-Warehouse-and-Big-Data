# MapReduce Pseudo-Code
# Customer Churn Analysis by Contract Type and Payment Method

# ---------------------------------------------------------
# MAP STAGE
# ---------------------------------------------------------

MAP(input_key, record):

    IF record.contract IS NOT NULL
       AND record.payment_method IS NOT NULL
       AND record.churn IN {0, 1}:

        key = (record.contract, record.payment_method)

        churn_value = record.churn

        EMIT(
            key,
            (1, churn_value)
        )


# ---------------------------------------------------------
# COMBINE STAGE
# ---------------------------------------------------------

COMBINE(key, values):

    local_customers = 0
    local_churned = 0

    FOR each (customer_count, churn_count) IN values:

        local_customers =
            local_customers + customer_count

        local_churned =
            local_churned + churn_count

    EMIT(
        key,
        (local_customers, local_churned)
    )


# ---------------------------------------------------------
# SHUFFLE AND SORT STAGE
# ---------------------------------------------------------

# This stage is performed automatically by the MapReduce
# framework.
#
# Intermediate records are partitioned and grouped by key.
# All values with the same:
#
#     (contract, payment_method)
#
# combination are sent to the same reducer.
#
# Example:
#
# Mapper 1:
# (Month-to-month, Electronic check) -> (3, 2)
#
# Mapper 2:
# (Month-to-month, Electronic check) -> (5, 1)
#
# Mapper 3:
# (Month-to-month, Electronic check) -> (2, 1)
#
# After Shuffle and Sort:
#
# (Month-to-month, Electronic check)
#     -> [(3, 2), (5, 1), (2, 1)]


# ---------------------------------------------------------
# REDUCE STAGE
# ---------------------------------------------------------

REDUCE(key, values):

    total_customers = 0
    total_churned = 0

    FOR each (customer_count, churn_count) IN values:

        total_customers =
            total_customers + customer_count

        total_churned =
            total_churned + churn_count

    IF total_customers > 0:

        churn_rate =
            (total_churned / total_customers) * 100

    ELSE:

        churn_rate = 0

    EMIT(
        key,
        (
            total_customers,
            total_churned,
            churn_rate
        )
    )


# ---------------------------------------------------------
# EXAMPLE OUTPUT
# ---------------------------------------------------------

# Key:
# (Month-to-month, Electronic check)
#
# Output:
# (
#     total_customers = 10000,
#     total_churned = 2600,
#     churn_rate = 26.0
# )


# ---------------------------------------------------------
# FINAL OUTPUT STRUCTURE
# ---------------------------------------------------------

# For each contract-payment combination:
#
# (
#     contract,
#     payment_method,
#     total_customers,
#     total_churned,
#     churn_rate
# )
