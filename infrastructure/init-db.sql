-- Initialize Airflow database
-- This file is executed when PostgreSQL container starts

-- Create additional schemas if needed
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Create data quality report table
CREATE TABLE IF NOT EXISTS monitoring.data_quality_report (
    id SERIAL PRIMARY KEY,
    report_date DATE,
    total_customers INTEGER,
    total_orders INTEGER,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    data_freshness_hours DECIMAL(6,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);