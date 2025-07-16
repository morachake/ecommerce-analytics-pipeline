{{ config(
    materialized='table',
    unique_key='customer_id',
    indexes=[
      {'columns': ['customer_segment']},
      {'columns': ['customer_status']},
      {'columns': ['last_order_date']}
    ]
) }}

WITH customer_orders AS (
    SELECT 
        dc.customer_id,
        dc.customer_segment,
        dc.registration_date,
        dc.age_group,
        dc.customer_status,
        dc.value_segment,
        
        -- Order metrics
        COUNT(fo.order_id) AS total_orders,
        SUM(fo.total_amount) AS total_spent,
        AVG(fo.total_amount) AS avg_order_value,
        MIN(fo.order_date) AS first_order_date,
        MAX(fo.order_date) AS last_order_date,
        
        -- Advanced metrics
        STDDEV(fo.total_amount) AS order_value_stddev,
        
        -- Payment method preferences
        MODE() WITHIN GROUP (ORDER BY fo.payment_method) AS preferred_payment_method,
        
        -- Recency, Frequency, Monetary (RFM) components
        CURRENT_DATE - MAX(fo.order_date) AS recency_days,
        COUNT(fo.order_id) AS frequency,
        SUM(fo.total_amount) AS monetary_value
        
    FROM {{ ref('dim_customers') }} dc
    LEFT JOIN {{ ref('fact_orders') }} fo 
        ON dc.customer_key = fo.customer_key
        AND fo.order_status = 'completed'
    WHERE dc.is_current = TRUE
    GROUP BY 
        dc.customer_id, dc.customer_segment, dc.registration_date,
        dc.age_group, dc.customer_status, dc.value_segment
),

customer_categories AS (
    SELECT 
        co.*,
        
        -- Category preferences from order items
        cp.preferred_category,
        cp.category_diversity,
        
        -- Customer lifetime calculations
        CASE 
            WHEN first_order_date IS NULL THEN 0
            WHEN last_order_date = first_order_date THEN 1
            ELSE ROUND(
                total_orders::NUMERIC / 
                GREATEST(1, (last_order_date - first_order_date + 1))::NUMERIC * 365, 2
            )
        END AS annual_order_frequency,
        
        -- Customer health score (0-100)
        LEAST(100, GREATEST(0, 
            CASE 
                WHEN total_orders = 0 THEN 0
                ELSE (
                    -- Recency score (0-40 points)
                    GREATEST(0, 40 - (recency_days * 40.0 / 365)) +
                    -- Frequency score (0-30 points)  
                    LEAST(30, frequency * 5) +
                    -- Monetary score (0-30 points)
                    LEAST(30, monetary_value / 50)
                )
            END
        )) AS customer_health_score
        
    FROM customer_orders co
    LEFT JOIN (
        SELECT 
            dc.customer_id,
            MODE() WITHIN GROUP (ORDER BY dp.category) AS preferred_category,
            COUNT(DISTINCT dp.category) AS category_diversity
        FROM {{ ref('dim_customers') }} dc
        JOIN {{ ref('fact_orders') }} fo ON dc.customer_key = fo.customer_key
        JOIN {{ ref('fact_order_items') }} foi ON fo.order_key = foi.order_key
        JOIN {{ ref('dim_products') }} dp ON foi.product_key = dp.product_key
        WHERE dc.is_current = TRUE AND fo.order_status = 'completed'
        GROUP BY dc.customer_id
    ) cp ON co.customer_id = cp.customer_id
),

rfm_scoring AS (
    SELECT 
        *,
        -- RFM Quintile scoring (1-5 scale)
        NTILE(5) OVER (ORDER BY recency_days DESC) AS recency_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary_value ASC) AS monetary_score
    FROM customer_categories
)

SELECT 
    customer_id,
    customer_segment,
    registration_date,
    age_group,
    customer_status,
    value_segment,
    
    -- Order metrics
    total_orders,
    total_spent,
    ROUND(avg_order_value, 2) AS avg_order_value,
    first_order_date,
    last_order_date,
    recency_days AS days_since_last_order,
    
    -- Advanced analytics
    ROUND(order_value_stddev, 2) AS order_value_consistency,
    preferred_payment_method,
    preferred_category,
    category_diversity,
    ROUND(annual_order_frequency, 2) AS annual_order_frequency,
    ROUND(customer_health_score, 1) AS customer_health_score,
    
    -- RFM Analysis
    recency_score,
    frequency_score,
    monetary_score,
    CONCAT(recency_score, frequency_score, monetary_score) AS rfm_segment,
    
    -- Customer lifecycle stage
    CASE 
        WHEN total_orders = 0 THEN 'Prospect'
        WHEN total_orders = 1 AND recency_days <= 30 THEN 'New Customer'
        WHEN total_orders = 1 AND recency_days > 30 THEN 'One-time Buyer'
        WHEN customer_health_score >= 70 THEN 'Champion'
        WHEN customer_health_score >= 50 THEN 'Loyal Customer'
        WHEN customer_health_score >= 30 THEN 'Potential Loyalist'
        WHEN recency_days <= 30 THEN 'New Customer'
        WHEN recency_days <= 90 THEN 'At Risk'
        ELSE 'Churned'
    END AS lifecycle_stage,
    
    -- Predicted next order (simple heuristic)
    CASE 
        WHEN total_orders <= 1 THEN NULL
        WHEN annual_order_frequency > 0 THEN 
            last_order_date + (365.0 / annual_order_frequency)::INTEGER
        ELSE NULL
    END AS predicted_next_order_date,
    
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM rfm_scoring
ORDER BY customer_health_score DESC, total_spent DESC