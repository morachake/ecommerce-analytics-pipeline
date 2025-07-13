-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw tables (landing zone)
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id INTEGER,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    registration_date DATE,
    customer_segment VARCHAR(50),
    birth_date DATE,
    gender VARCHAR(10),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id INTEGER,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    weight DECIMAL(8,2),
    dimensions VARCHAR(50),
    description TEXT,
    created_date DATE,
    is_active BOOLEAN,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    shipping_method VARCHAR(50),
    shipping_cost DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    currency VARCHAR(10),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.web_events (
    event_id INTEGER,
    customer_id INTEGER,
    session_id VARCHAR(100),
    event_type VARCHAR(50),
    product_id INTEGER,
    event_timestamp TIMESTAMP,
    user_agent TEXT,
    ip_address VARCHAR(50),
    referrer TEXT,
    page_url TEXT,
    user_type VARCHAR(50),
    device_type VARCHAR(50),
    country VARCHAR(10),
    city VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255)
);

-- Staging tables (cleaned and validated)
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    registration_date DATE,
    customer_segment VARCHAR(50),
    birth_date DATE,
    gender VARCHAR(10),
    age_group VARCHAR(20),
    dbt_valid_from TIMESTAMP,
    dbt_valid_to TIMESTAMP,
    dbt_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    margin_percent DECIMAL(5,2),
    weight DECIMAL(8,2),
    dimensions VARCHAR(50),
    description TEXT,
    created_date DATE,
    is_active BOOLEAN,
    price_tier VARCHAR(20),
    dbt_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Warehouse dimension tables
CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    registration_date DATE,
    customer_segment VARCHAR(50),
    birth_date DATE,
    gender VARCHAR(10),
    age_group VARCHAR(20),
    customer_lifetime_value DECIMAL(12,2),
    total_orders INTEGER,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    dbt_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER UNIQUE,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    margin_percent DECIMAL(5,2),
    weight DECIMAL(8,2),
    dimensions VARCHAR(50),
    description TEXT,
    created_date DATE,
    is_active BOOLEAN,
    price_tier VARCHAR(20),
    dbt_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    date_actual DATE,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_actual INTEGER,
    month_name VARCHAR(20),
    quarter_actual INTEGER,
    year_actual INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Warehouse fact table
CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id INTEGER,
    customer_key INTEGER,
    date_key INTEGER,
    order_date DATE,
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    shipping_method VARCHAR(50),
    shipping_cost DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    subtotal_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    currency VARCHAR(10),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (customer_key) REFERENCES warehouse.dim_customers(customer_key),
    FOREIGN KEY (date_key) REFERENCES warehouse.dim_date(date_key)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    order_item_key SERIAL PRIMARY KEY,
    order_item_id INTEGER,
    order_key INTEGER,
    product_key INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    created_at TIMESTAMP,
    FOREIGN KEY (order_key) REFERENCES warehouse.fact_orders(order_key),
    FOREIGN KEY (product_key) REFERENCES warehouse.dim_products(product_key)
);

-- Mart tables (business-ready aggregations)
CREATE TABLE IF NOT EXISTS marts.customer_summary (
    customer_id INTEGER PRIMARY KEY,
    first_order_date DATE,
    last_order_date DATE,
    total_orders INTEGER,
    total_spent DECIMAL(12,2),
    average_order_value DECIMAL(10,2),
    days_since_last_order INTEGER,
    customer_lifetime_value DECIMAL(12,2),
    is_active BOOLEAN,
    customer_segment VARCHAR(50),
    preferred_category VARCHAR(100),
    dbt_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS marts.product_performance (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    total_quantity_sold INTEGER,
    total_revenue DECIMAL(12,2),
    average_rating DECIMAL(3,2),
    number_of_orders INTEGER,
    inventory_turns DECIMAL(8,2),
    profit_margin DECIMAL(5,2),
    last_order_date DATE,
    dbt_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_customers_loaded_at ON raw.customers(loaded_at);
CREATE INDEX IF NOT EXISTS idx_raw_orders_order_date ON raw.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_raw_order_items_order_id ON raw.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_raw_web_events_timestamp ON raw.web_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_key ON warehouse.fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date_key ON warehouse.fact_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_product_key ON warehouse.fact_order_items(product_key);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO warehouse;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO warehouse;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO warehouse;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts TO warehouse;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA warehouse TO warehouse;