{{ config(
    materialized='table',
    unique_key='customer_key',
    indexes=[
      {'columns': ['customer_id'], 'unique': True},
      {'columns': ['customer_segment']},
      {'columns': ['registration_date']}
    ]
) }}

WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COALESCE(SUM(o.total_amount), 0) AS total_spent,
        COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        CURRENT_DATE - MAX(o.order_date) AS days_since_last_order
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_orders') }} o 
        ON c.customer_id = o.customer_id 
        AND o.order_status = 'completed'
    GROUP BY c.customer_id
),

customer_lifetime_value AS (
    SELECT 
        customer_id,
        CASE 
            WHEN total_orders = 0 THEN 0
            WHEN days_since_last_order IS NULL THEN total_spent
            WHEN days_since_last_order <= 90 THEN 
                total_spent * (1 + (total_orders * 0.1))
            ELSE total_spent * 0.8
        END AS customer_lifetime_value
    FROM customer_metrics
),

final AS (
    SELECT 
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} AS customer_key,
        
        -- Customer attributes
        c.customer_id,
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.phone,
        c.address,
        c.city,
        c.state,
        c.zip_code,
        c.country,
        c.registration_date,
        c.customer_segment,
        c.birth_date,
        c.gender,
        c.age_group,
        c.age,
        c.days_since_registration,
        
        -- Customer metrics
        COALESCE(m.total_orders, 0) AS total_orders,
        COALESCE(m.total_spent, 0) AS total_spent,
        COALESCE(m.avg_order_value, 0) AS avg_order_value,
        m.first_order_date,
        m.last_order_date,
        m.days_since_last_order,
        COALESCE(clv.customer_lifetime_value, 0) AS customer_lifetime_value,
        
        -- Customer status
        CASE 
            WHEN m.total_orders = 0 THEN 'Prospect'
            WHEN m.days_since_last_order <= 30 THEN 'Active'
            WHEN m.days_since_last_order <= 90 THEN 'At Risk'
            ELSE 'Churned'
        END AS customer_status,
        
        CASE 
            WHEN m.total_spent >= 1000 THEN 'High Value'
            WHEN m.total_spent >= 500 THEN 'Medium Value'
            WHEN m.total_spent > 0 THEN 'Low Value'
            ELSE 'No Value'
        END AS value_segment,
        
        -- SCD Type 2 fields
        c.loaded_at AS valid_from,
        NULL AS valid_to,
        TRUE AS is_current,
        
        -- Metadata
        c.dbt_updated_at
        
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN customer_metrics m ON c.customer_id = m.customer_id
    LEFT JOIN customer_lifetime_value clv ON c.customer_id = clv.customer_id
)

SELECT * FROM final