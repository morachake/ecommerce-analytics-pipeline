CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id INTEGER,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    zip_code VARCHAR(255),
    country VARCHAR(255),
    registration_date DATE,
    customer_segment VARCHAR(255),
    birth_date DATE,
    gender VARCHAR(255),
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
)

CREATE TABLE IF NOT EXISTS raw.products (
    product_id INTEGER,
    product_name VARCHAR(255),
    category VARCHAR(255),
    sub_category VARCHAR(255),
    brand VARCHAR(255),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    weight DECIMAL(10, 2),
    dimensions VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
)

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(255),
    payment_method VARCHAR(255),
    shipping_method VARCHAR(255),
    shipping_cost DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    currency VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
)

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    line_total DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
)

CREATE TABLE IF NOT EXISTS raw.web_events (
    event_id INTEGER,
    customer_id INTEGER,
    session_id INTEGER,
    event_type VARCHAR(255),
    product_id INTEGER,
    event_time TIMESTAMP ,
    user_agent VARCHAR(255),
    ip_address VARCHAR(255),
    referrer VARCHAR(255),
    page_url TEXT,
    user_type VARCHAR(255),
    device_type VARCHAR(255),
    country VARCHAR(255),
    city VARCHAR(255),
    loaded_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
)

Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_customers_loaded_at ON raw.customers(loaded_at);
CREATE INDEX IF NOT EXISTS idx_raw_orders_order_date ON raw.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_raw_order_items_order_id ON raw.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_raw_web_events_timestamp ON raw.web_events(event_timestamp);



GRANT ALL PRIVILEGES ON SCHEMA raw TO warehouse;
GRANT ALL PRIVILEGES ON SCHEMA staging TO warehouse;
GRANT ALL PRIVILEGES ON SCHEMA warehouse TO warehouse;
GRANT ALL PRIVILEGES ON SCHEMA marts TO warehouse;
GRANT ON ALL SEQUENCES IN SCHEMA warehouse TO warehouse


GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO warehouse;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO warehouse;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO warehouse;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts TO warehouse;