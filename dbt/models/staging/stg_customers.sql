{{ config(
    materialized="view",
    unique_key="customer_id"
) }}

WITH source_data AS (
    SELECT 
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        phone,
        address,
        city,
        state,
        zip_code,
        country,
        registration_date,
        customer_segment,
        birth_date,
        UPPER(gender) AS gender,
        loaded_at
    FROM {{ source("raw", "customers") }}
    WHERE customer_id IS NOT NULL
        AND email IS NOT NULL
        AND email LIKE '%@%'
),

enriched_data AS (
    SELECT 
        *,
        CONCAT(first_name, ' ', last_name) AS full_name,
        CASE 
            WHEN EXTRACT(YEAR FROM AGE(birth_date)) BETWEEN 18 AND 25 THEN '18-25'
            WHEN EXTRACT(YEAR FROM AGE(birth_date)) BETWEEN 26 AND 35 THEN '26-35'
            WHEN EXTRACT(YEAR FROM AGE(birth_date)) BETWEEN 36 AND 45 THEN '36-45'
            WHEN EXTRACT(YEAR FROM AGE(birth_date)) BETWEEN 46 AND 55 THEN '46-55'
            WHEN EXTRACT(YEAR FROM AGE(birth_date)) BETWEEN 56 AND 65 THEN '56-65'
            ELSE '65+'
        END AS age_group,
        EXTRACT(YEAR FROM AGE(birth_date)) AS age,
        (CURRENT_DATE - registration_date) AS days_since_registration
    FROM source_data
)

SELECT 
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    address,
    city,
    state,
    zip_code,
    country,
    registration_date,
    customer_segment,
    birth_date,
    gender,
    age_group,
    age,
    days_since_registration,
    loaded_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM enriched_data
