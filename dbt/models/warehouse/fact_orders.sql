{{ config(
    materialized='table',
    unique_key='order_key',
    indexes=[
      {'columns': ['order_id'], 'unique': True},
      {'columns': ['customer_key']},
      {'columns': ['date_key']},
      {'columns': ['order_date']}
    ]
) }}

WITH order_enriched AS (
    SELECT 
        o.*,
        dc.customer_key,
        dd.date_key
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('dim_customers') }} dc 
        ON o.customer_id = dc.customer_id 
        AND dc.is_current = TRUE
    LEFT JOIN {{ ref('dim_date') }} dd 
        ON o.order_date = dd.date_actual
)

SELECT 
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key,
    
    -- Business keys
    order_id,
    customer_key,
    date_key,
    
    -- Order attributes
    order_date,
    order_status,
    payment_method,
    shipping_method,
    order_value_tier,
    day_of_week,
    hour_of_day,
    day_type,
    month_period,
    
    -- Financial metrics
    shipping_cost,
    tax_amount,
    subtotal_amount,
    total_amount,
    currency,
    
    -- Calculated metrics
    CASE 
        WHEN subtotal_amount > 0 THEN (tax_amount / subtotal_amount) * 100 
        ELSE 0 
    END AS tax_rate_percent,
    
    CASE 
        WHEN subtotal_amount > 0 THEN (shipping_cost / subtotal_amount) * 100 
        ELSE 0 
    END AS shipping_rate_percent,
    
    -- Timestamps
    created_at,
    updated_at,
    dbt_updated_at
    
FROM order_enriched
WHERE customer_key IS NOT NULL