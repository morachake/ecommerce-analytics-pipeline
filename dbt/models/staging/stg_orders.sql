{{ config(
    materialized='view',
    unique_key='order_id'
) }}

WITH source_data AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        LOWER(order_status) AS order_status,
        payment_method,
        shipping_method,
        shipping_cost,
        tax_amount,
        total_amount,
        currency,
        created_at,
        updated_at,
        loaded_at
    FROM {{ source('raw', 'orders') }}
    WHERE order_id IS NOT NULL
        AND customer_id IS NOT NULL
        AND order_date IS NOT NULL
        AND total_amount >= {{ var('min_order_value') }}
        AND total_amount <= {{ var('max_order_value') }}
),

enriched_data AS (
    SELECT 
        *,
        total_amount - tax_amount - shipping_cost AS subtotal_amount,
        CASE 
            WHEN total_amount < 50 THEN 'Low'
            WHEN total_amount < 200 THEN 'Medium'
            WHEN total_amount < 500 THEN 'High'
            ELSE 'Premium'
        END AS order_value_tier,
        EXTRACT(DOW FROM order_date) AS day_of_week,
        EXTRACT(HOUR FROM created_at) AS hour_of_day,
        CASE 
            WHEN EXTRACT(DOW FROM order_date) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        CASE 
            WHEN order_date BETWEEN DATE_TRUNC('month', order_date) + INTERVAL '22 days' 
                              AND DATE_TRUNC('month', order_date) + INTERVAL '1 month - 1 day'
            THEN 'End of Month'
            WHEN EXTRACT(DAY FROM order_date) <= 7 THEN 'Beginning of Month'
            ELSE 'Mid Month'
        END AS month_period
    FROM source_data
)

SELECT 
    order_id,
    customer_id,
    order_date,
    order_status,
    payment_method,
    shipping_method,
    shipping_cost,
    tax_amount,
    subtotal_amount,
    total_amount,
    currency,
    order_value_tier,
    day_of_week,
    hour_of_day,
    day_type,
    month_period,
    created_at,
    updated_at,
    loaded_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM enriched_data