-- ======================================================
-- SCHEMA SILVER - CLEAN (Tanpa OPS - hanya silver.*)
-- ======================================================
CREATE SCHEMA IF NOT EXISTS silver;

-- 1. SILVER CUSTOMERS
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id VARCHAR(100) PRIMARY KEY,
    email VARCHAR(255), full_name VARCHAR(255), phone VARCHAR(50), city_id VARCHAR(100),
    customer_segment TEXT, created_at_utc TIMESTAMPTZ, valid_from_utc TIMESTAMPTZ, valid_to_utc TIMESTAMPTZ,
    source_row_id TEXT, bronze_file_name VARCHAR(255), bronze_row_number INT,
    bronze_source_record_id VARCHAR(255), bronze_checksum VARCHAR(128),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(), is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMPTZ DEFAULT NOW(), effective_to TIMESTAMPTZ,
    version_number INT DEFAULT 1, is_orphan BOOLEAN DEFAULT FALSE, deterministic_key VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS silver.customer_addresses (
    address_id VARCHAR(100) PRIMARY KEY, customer_id VARCHAR(100), city_id VARCHAR(100),
    valid_from_utc TIMESTAMPTZ, valid_to_utc TIMESTAMPTZ, source_row_id TEXT,
    bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW(), is_current BOOLEAN DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS silver.cities (
    city_id VARCHAR(100) PRIMARY KEY, city_name TEXT, country TEXT,
    source_row_id TEXT, bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS silver.products (
    product_id VARCHAR(100) PRIMARY KEY, sku VARCHAR(100) UNIQUE, product_name TEXT,
    category_id VARCHAR(100), category_name TEXT, price NUMERIC(14,2) CHECK (price >= 0),
    unit_price NUMERIC(14,2), source_row_id TEXT, bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(), is_current BOOLEAN DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS silver.product_categories (
    product_id VARCHAR(100), category_id VARCHAR(100), category_name TEXT,
    valid_from_utc TIMESTAMPTZ, valid_to_utc TIMESTAMPTZ, source_row_id TEXT,
    bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (product_id, category_id)
);
CREATE TABLE IF NOT EXISTS silver.promotions (
    promotion_id VARCHAR(100) PRIMARY KEY, promotion_code TEXT, promotion_type TEXT,
    discount_rate NUMERIC(10,4), discount_amount NUMERIC(14,2), start_date DATE, end_date DATE,
    source_row_id TEXT, bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS silver.stores (
    store_id VARCHAR(100) PRIMARY KEY, store_name TEXT, city_id VARCHAR(100),
    location_type TEXT, source_row_id TEXT, bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS silver.channels (
    channel_id VARCHAR(100) PRIMARY KEY, channel_name TEXT, source_row_id TEXT,
    bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id VARCHAR(100) PRIMARY KEY, customer_id VARCHAR(100), store_id VARCHAR(100), channel_id VARCHAR(100),
    sales_channel TEXT DEFAULT 'online', order_status TEXT, status TEXT,
    order_date_utc TIMESTAMPTZ, ordered_at_utc TIMESTAMPTZ, updated_at_utc TIMESTAMPTZ,
    total_amount NUMERIC(14,2), shipping_cost NUMERIC(14,2) DEFAULT 0, shipping_revenue NUMERIC(14,2) DEFAULT 0,
    return_status TEXT DEFAULT 'NONE', source_row_id TEXT, bronze_file_name VARCHAR(255), bronze_row_number INT,
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(), is_current BOOLEAN DEFAULT TRUE,
    is_late_arriving BOOLEAN DEFAULT FALSE, is_orphan BOOLEAN DEFAULT FALSE, deterministic_key VARCHAR(512),
    order_date DATE
);
CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id VARCHAR(100) PRIMARY KEY, order_id VARCHAR(100), product_id VARCHAR(100),
    quantity INT, unit_price NUMERIC(14,2), item_discount_amount NUMERIC(14,2) DEFAULT 0,
    discount_amount NUMERIC(14,2) DEFAULT 0, subtotal NUMERIC(14,2), sales_channel TEXT DEFAULT 'online',
    promotion_id VARCHAR(100), source_row_id TEXT, bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(), is_orphan BOOLEAN DEFAULT FALSE, deterministic_key VARCHAR(512)
);
CREATE TABLE IF NOT EXISTS silver.payment_events (
    payment_id VARCHAR(100) PRIMARY KEY, order_id VARCHAR(100), event_id TEXT UNIQUE, event_type TEXT,
    ingested_at_utc TIMESTAMPTZ, occurred_at_utc TIMESTAMPTZ, payment_event_time TIMESTAMPTZ,
    amount NUMERIC(14,2), payment_amount NUMERIC(14,2), payment_status TEXT, payment_method TEXT,
    source_row_id TEXT, bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    is_current BOOLEAN DEFAULT TRUE, deterministic_key VARCHAR(512)
);
CREATE TABLE IF NOT EXISTS silver.payments (
    payment_id VARCHAR(100) PRIMARY KEY, order_id VARCHAR(100), event_id TEXT UNIQUE,
    payment_event_time TIMESTAMPTZ, payment_amount NUMERIC(14,2), amount NUMERIC(14,2),
    payment_method TEXT, payment_status TEXT, event_type TEXT,
    bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS silver.refunds (
    refund_id VARCHAR(100) PRIMARY KEY, order_id VARCHAR(100), event_id TEXT, event_type TEXT,
    amount NUMERIC(14,2), ingested_at_utc TIMESTAMPTZ, occurred_at_utc TIMESTAMPTZ,
    source_row_id TEXT, bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS silver.returns (
    return_id VARCHAR(100) PRIMARY KEY, order_id VARCHAR(100), event_id TEXT, event_type TEXT,
    ingested_at_utc TIMESTAMPTZ, occurred_at_utc TIMESTAMPTZ, source_row_id TEXT,
    bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS silver.support_tickets (
    ticket_id VARCHAR(100) PRIMARY KEY, customer_id VARCHAR(100), order_id VARCHAR(100), reason TEXT,
    event_id TEXT, event_type TEXT, ingested_at_utc TIMESTAMPTZ, occurred_at_utc TIMESTAMPTZ,
    source_row_id TEXT, bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS silver.web_events (
    event_id VARCHAR(100) PRIMARY KEY, session_id VARCHAR(100), customer_id VARCHAR(100), order_id VARCHAR(100),
    campaign_id VARCHAR(100), channel TEXT, sales_channel TEXT, event_type TEXT,
    ingested_at_utc TIMESTAMPTZ, occurred_at_utc TIMESTAMPTZ, event_timestamp_utc TIMESTAMPTZ,
    page_url TEXT, source_row_id TEXT, bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    deterministic_key VARCHAR(512), is_current BOOLEAN DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS silver.order_promotions (
    order_id VARCHAR(100), promotion_id VARCHAR(100), discount_amount NUMERIC(14,2),
    source_row_id TEXT, bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (order_id, promotion_id)
);
CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
    inventory_id VARCHAR(100), product_id VARCHAR(100), store_id VARCHAR(100),
    stock_quantity INT, snapshot_date_utc DATE, bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(), is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(255), snapshot_timestamp_utc TIMESTAMPTZ,
    PRIMARY KEY (inventory_id, snapshot_date_utc)
);
CREATE TABLE IF NOT EXISTS silver.campaign_spend (
    campaign_id VARCHAR(100), spend_date_utc DATE NOT NULL, channel_id VARCHAR(100),
    spend_amount NUMERIC(14,2), bronze_file_name VARCHAR(255), silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    is_current BOOLEAN DEFAULT TRUE, deterministic_key VARCHAR(512), snapshot_timestamp_utc TIMESTAMPTZ,
    PRIMARY KEY (campaign_id, spend_date_utc)
);
CREATE TABLE IF NOT EXISTS silver.order_items_pending (
    order_item_id VARCHAR(100) PRIMARY KEY, order_id VARCHAR(100) NOT NULL, product_id VARCHAR(100) NOT NULL,
    quantity INT NOT NULL, unit_price NUMERIC(14,2), item_discount_amount NUMERIC(14,2) DEFAULT 0,
    bronze_file_name VARCHAR(255), status TEXT DEFAULT 'PENDING', is_orphan BOOLEAN DEFAULT TRUE,
    ingested_at TIMESTAMPTZ DEFAULT NOW(), parent_search_deadline TIMESTAMPTZ DEFAULT NOW() + INTERVAL '3 days'
);
CREATE TABLE IF NOT EXISTS silver.rejected_records (
    rejection_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100) NOT NULL, file_name VARCHAR(255),
    row_number INT, source_record_id VARCHAR(255), rejection_stage VARCHAR(50), rejection_reason TEXT,
    failed_field VARCHAR(100), failed_value TEXT, payload_json JSONB, bronze_checksum VARCHAR(128),
    rejected_at TIMESTAMPTZ DEFAULT NOW(), is_resolved BOOLEAN DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS silver.write_silver (
    write_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100) NOT NULL, records_in INT, records_out INT,
    records_rejected INT, status VARCHAR(20) DEFAULT 'SUCCESS', business_date DATE,
    bronze_file_name VARCHAR(255), finished_at TIMESTAMPTZ DEFAULT NOW(), message TEXT
);
CREATE TABLE IF NOT EXISTS silver.silver_late_arriving_buffer (
    buffer_id BIGSERIAL PRIMARY KEY, target_table TEXT, business_key TEXT,
    payload_json JSONB, status TEXT, deterministic_key TEXT
);

-- INDEX
CREATE INDEX IF NOT EXISTS idx_orders_customer ON silver.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON silver.orders(order_date_utc);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON silver.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_payment_order ON silver.payment_events(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_order ON silver.payments(order_id);
CREATE INDEX IF NOT EXISTS idx_refunds_order ON silver.refunds(order_id);
CREATE INDEX IF NOT EXISTS idx_returns_order ON silver.returns(order_id);
CREATE INDEX IF NOT EXISTS idx_web_events_order ON silver.web_events(order_id);
CREATE INDEX IF NOT EXISTS idx_web_events_event_type ON silver.web_events(event_type);

CREATE OR REPLACE VIEW silver.fact_return AS
SELECT order_id, MAX(is_returned)::INT as is_returned FROM (
  SELECT order_id, CASE WHEN UPPER(return_status) != 'NONE' THEN 1 ELSE 0 END as is_returned FROM silver.orders WHERE order_id IS NOT NULL
  UNION ALL SELECT order_id, 1 FROM silver.returns WHERE order_id IS NOT NULL
  UNION ALL SELECT order_id, 1 FROM silver.web_events WHERE order_id IS NOT NULL AND UPPER(event_type) LIKE '%RETURN%'
) t GROUP BY order_id;

-- UNIQUE INDEX
CREATE UNIQUE INDEX IF NOT EXISTS uq_order_items_order_item_id ON silver.order_items (order_item_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_events_payment_id ON silver.payment_events (payment_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_events_event_id ON silver.payment_events (event_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_payments_payment_id ON silver.payments (payment_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_payments_event_id ON silver.payments (event_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_customers_customer_id ON silver.customers (customer_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_order_id ON silver.orders (order_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_products_product_id ON silver.products (product_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_stores_store_id ON silver.stores (store_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_web_events_event_id ON silver.web_events (event_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_inventory_id_date ON silver.inventory_snapshots (inventory_id, snapshot_date_utc);
CREATE UNIQUE INDEX IF NOT EXISTS uq_campaign_spend_pk ON silver.campaign_spend (campaign_id, spend_date_utc);

ALTER TABLE silver.order_items DROP CONSTRAINT IF EXISTS order_items_quantity_check;
ALTER TABLE silver.order_items DROP CONSTRAINT IF EXISTS silver_order_items_quantity_check;
ALTER TABLE silver.order_items DROP CONSTRAINT IF EXISTS order_items_quantity_check1;
