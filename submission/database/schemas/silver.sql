-- ======================================================
-- SCHEMA SILVER - FIXED & LENGKAP (Bronze -> Silver)
-- ======================================================
CREATE SCHEMA IF NOT EXISTS silver;

-- 1. SILVER CUSTOMERS (SCD2 dari PROFILE + CUSTOMER)
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id VARCHAR(100) PRIMARY KEY,
    email VARCHAR(255),
    full_name VARCHAR(255),
    phone VARCHAR(50),
    city_id VARCHAR(100),
    customer_segment TEXT,
    created_at_utc TIMESTAMPTZ,
    valid_from_utc TIMESTAMPTZ,
    valid_to_utc TIMESTAMPTZ,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMPTZ DEFAULT NOW(),
    effective_to TIMESTAMPTZ,
    version_number INT DEFAULT 1,
    is_orphan BOOLEAN DEFAULT FALSE,
    deterministic_key VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS ops.silver_to_gold_quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    target_table TEXT,
    business_key TEXT,
    order_id TEXT,
    reason TEXT,
    failed_field TEXT,
    failed_value TEXT,
    payload_json JSONB,
    quarantined_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. SILVER CUSTOMER_ADDRESSES
CREATE TABLE IF NOT EXISTS silver.customer_addresses (
    address_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) REFERENCES silver.customers(customer_id),
    city_id VARCHAR(100),
    valid_from_utc TIMESTAMPTZ,
    valid_to_utc TIMESTAMPTZ,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    is_current BOOLEAN DEFAULT TRUE
);

-- 3. SILVER CITIES
CREATE TABLE IF NOT EXISTS silver.cities (
    city_id VARCHAR(100) PRIMARY KEY,
    city_name TEXT,
    country TEXT,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. SILVER PRODUCTS
CREATE TABLE IF NOT EXISTS silver.products (
    product_id VARCHAR(100) PRIMARY KEY,
    sku VARCHAR(100) UNIQUE,
    product_name TEXT,
    category_id VARCHAR(100),
    category_name TEXT,
    price NUMERIC(14,2) CHECK (price >= 0),
    unit_price NUMERIC(14,2),
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    is_current BOOLEAN DEFAULT TRUE
);

-- 5. SILVER PRODUCT_CATEGORIES
CREATE TABLE IF NOT EXISTS silver.product_categories (
    product_id VARCHAR(100),
    category_id VARCHAR(100),
    category_name TEXT,
    valid_from_utc TIMESTAMPTZ,
    valid_to_utc TIMESTAMPTZ,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (product_id, category_id)
);

-- 6. SILVER PROMOTIONS
CREATE TABLE IF NOT EXISTS silver.promotions (
    promotion_id VARCHAR(100) PRIMARY KEY,
    promotion_code TEXT,
    promotion_type TEXT,
    discount_rate NUMERIC(10,4),
    discount_amount NUMERIC(14,2),
    start_date DATE,
    end_date DATE,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- 7. SILVER STORES & CHANNELS
CREATE TABLE IF NOT EXISTS silver.stores (
    store_id VARCHAR(100) PRIMARY KEY,
    store_name TEXT,
    city_id VARCHAR(100),
    location_type TEXT,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.channels (
    channel_id VARCHAR(100) PRIMARY KEY,
    channel_name TEXT,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- 8. SILVER ORDERS - LENGKAP
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) REFERENCES silver.customers(customer_id),
    store_id VARCHAR(100),
    channel_id VARCHAR(100),
    sales_channel TEXT DEFAULT 'online',
    order_status TEXT,
    status TEXT,
    order_date_utc TIMESTAMPTZ,
    ordered_at_utc TIMESTAMPTZ,
    updated_at_utc TIMESTAMPTZ,
    total_amount NUMERIC(14,2),
    shipping_cost NUMERIC(14,2) DEFAULT 0,
    shipping_revenue NUMERIC(14,2) DEFAULT 0,
    return_status TEXT DEFAULT 'NONE',
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    is_current BOOLEAN DEFAULT TRUE,
    is_late_arriving BOOLEAN DEFAULT FALSE,
    is_orphan BOOLEAN DEFAULT FALSE,
    deterministic_key VARCHAR(512)
);

-- 9. SILVER ORDER_ITEMS - LENGKAP
CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) REFERENCES silver.orders(order_id),
    product_id VARCHAR(100) REFERENCES silver.products(product_id),
    quantity INT CHECK (quantity > 0),
    unit_price NUMERIC(14,2),
    item_discount_amount NUMERIC(14,2) DEFAULT 0,
    discount_amount NUMERIC(14,2) DEFAULT 0,
    subtotal NUMERIC(14,2),
    sales_channel TEXT DEFAULT 'online',
    promotion_id VARCHAR(100),
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    is_orphan BOOLEAN DEFAULT FALSE,
    deterministic_key VARCHAR(512)
);

-- 10. SILVER PAYMENT_EVENTS
CREATE TABLE IF NOT EXISTS silver.payment_events (
    payment_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) REFERENCES silver.orders(order_id),
    event_id TEXT UNIQUE,
    event_type TEXT,
    ingested_at_utc TIMESTAMPTZ,
    occurred_at_utc TIMESTAMPTZ,
    amount NUMERIC(14,2),
    payment_amount NUMERIC(14,2),
    payment_status TEXT,
    payment_method TEXT,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    is_current BOOLEAN DEFAULT TRUE
);

-- 11. SILVER REFUNDS (BARU - dari list refund_id)
CREATE TABLE IF NOT EXISTS silver.refunds (
    refund_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) REFERENCES silver.orders(order_id),
    event_id TEXT,
    event_type TEXT,
    amount NUMERIC(14,2),
    ingested_at_utc TIMESTAMPTZ,
    occurred_at_utc TIMESTAMPTZ,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- 12. SILVER RETURNS (BARU - dari list return_id)
CREATE TABLE IF NOT EXISTS silver.returns (
    return_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) REFERENCES silver.orders(order_id),
    event_id TEXT,
    event_type TEXT,
    ingested_at_utc TIMESTAMPTZ,
    occurred_at_utc TIMESTAMPTZ,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- 13. SILVER SUPPORT_TICKETS (BARU - dari list ticket_id, reason)
CREATE TABLE IF NOT EXISTS silver.support_tickets (
    ticket_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100),
    order_id VARCHAR(100),
    reason TEXT,
    event_id TEXT,
    event_type TEXT,
    ingested_at_utc TIMESTAMPTZ,
    occurred_at_utc TIMESTAMPTZ,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- 14. SILVER CART_EVENTS / WEB_EVENTS (dari campaign_id, channel, session_id)
CREATE TABLE IF NOT EXISTS silver.web_events (
    event_id VARCHAR(100) PRIMARY KEY,
    session_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_id VARCHAR(100),
    campaign_id VARCHAR(100),
    channel TEXT,
    sales_channel TEXT,
    event_type TEXT,
    ingested_at_utc TIMESTAMPTZ,
    occurred_at_utc TIMESTAMPTZ,
    event_timestamp_utc TIMESTAMPTZ,
    page_url TEXT,
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- 15. SILVER ORDER_PROMOTIONS
CREATE TABLE IF NOT EXISTS silver.order_promotions (
    order_id VARCHAR(100),
    promotion_id VARCHAR(100),
    discount_amount NUMERIC(14,2),
    source_row_id TEXT,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (order_id, promotion_id)
);

-- 16. SILVER INVENTORY & CAMPAIGN
CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
    inventory_id VARCHAR(100),
    product_id VARCHAR(100) REFERENCES silver.products(product_id),
    store_id VARCHAR(100),
    stock_quantity INT CHECK (stock_quantity >= 0),
    snapshot_date_utc DATE,
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    is_current BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (inventory_id, snapshot_date_utc)
);

CREATE TABLE IF NOT EXISTS silver.campaign_spend (
    campaign_id VARCHAR(100),
    spend_date_utc DATE NOT NULL,
    channel_id VARCHAR(100),
    spend_amount NUMERIC(14,2),
    bronze_file_name VARCHAR(255),
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    is_current BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (campaign_id, spend_date_utc)
);

-- 17. PENDING & AUDIT
CREATE TABLE IF NOT EXISTS silver.order_items_pending (
    order_item_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(14,2),
    item_discount_amount NUMERIC(14,2) DEFAULT 0,
    bronze_file_name VARCHAR(255),
    status TEXT DEFAULT 'PENDING',
    is_orphan BOOLEAN DEFAULT TRUE,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    parent_search_deadline TIMESTAMPTZ DEFAULT NOW() + INTERVAL '3 days'
);

CREATE TABLE IF NOT EXISTS silver.rejected_records (
    rejection_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100) NOT NULL,
    file_name VARCHAR(255),
    row_number INT,
    source_record_id VARCHAR(255),
    rejection_stage VARCHAR(50),
    rejection_reason TEXT,
    failed_field VARCHAR(100),
    failed_value TEXT,
    payload_json JSONB,
    bronze_checksum VARCHAR(128),
    rejected_at TIMESTAMPTZ DEFAULT NOW(),
    is_resolved BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS silver.write_silver (
    write_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100) NOT NULL,
    records_in INT,
    records_out INT,
    records_rejected INT,
    status VARCHAR(20) DEFAULT 'SUCCESS',
    business_date DATE,
    bronze_file_name VARCHAR(255),
    finished_at TIMESTAMPTZ DEFAULT NOW(),
    message TEXT
);

-- ==========================================
-- 18. INDEX & VIEW FACT_RETURN
-- ==========================================
CREATE INDEX IF NOT EXISTS idx_orders_customer ON silver.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON silver.orders(order_date_utc);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON silver.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_payment_order ON silver.payment_events(order_id);
CREATE INDEX IF NOT EXISTS idx_refunds_order ON silver.refunds(order_id);
CREATE INDEX IF NOT EXISTS idx_returns_order ON silver.returns(order_id);
CREATE INDEX IF NOT EXISTS idx_web_events_order ON silver.web_events(order_id);
CREATE INDEX IF NOT EXISTS idx_web_events_event_type ON silver.web_events(event_type);

CREATE OR REPLACE VIEW silver.fact_return AS
SELECT order_id, MAX(is_returned)::INT as is_returned FROM (
  SELECT order_id, CASE WHEN UPPER(return_status) != 'NONE' THEN 1 ELSE 0 END as is_returned FROM silver.orders WHERE order_id IS NOT NULL
  UNION ALL
  SELECT order_id, 1 as is_returned FROM silver.returns WHERE order_id IS NOT NULL
  UNION ALL
  SELECT order_id, 1 as is_returned FROM silver.web_events WHERE order_id IS NOT NULL AND UPPER(event_type) LIKE '%RETURN%'
) t GROUP BY order_id;

-- ==========================================
-- 19. MIGRASI BACKFILL JIKA KOLOM BARU (IDEMPOTENT)
-- ==========================================
ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS sales_channel TEXT DEFAULT 'online';
ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS shipping_cost NUMERIC(14,2) DEFAULT 0;
ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS shipping_revenue NUMERIC(14,2) DEFAULT 0;
ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS return_status TEXT DEFAULT 'NONE';
ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS ordered_at_utc TIMESTAMPTZ;
ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS updated_at_utc TIMESTAMPTZ;
ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS source_row_id TEXT;

ALTER TABLE silver.order_items ADD COLUMN IF NOT EXISTS item_discount_amount NUMERIC(14,2) DEFAULT 0;
ALTER TABLE silver.order_items ADD COLUMN IF NOT EXISTS discount_amount NUMERIC(14,2) DEFAULT 0;
ALTER TABLE silver.order_items ADD COLUMN IF NOT EXISTS sales_channel TEXT DEFAULT 'online';
ALTER TABLE silver.order_items ADD COLUMN IF NOT EXISTS promotion_id VARCHAR(100);
ALTER TABLE silver.order_items ADD COLUMN IF NOT EXISTS source_row_id TEXT;

ALTER TABLE silver.web_events ADD COLUMN IF NOT EXISTS order_id VARCHAR(100);
ALTER TABLE silver.web_events ADD COLUMN IF NOT EXISTS campaign_id VARCHAR(100);
ALTER TABLE silver.web_events ADD COLUMN IF NOT EXISTS channel TEXT;
ALTER TABLE silver.web_events ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ;
ALTER TABLE silver.web_events ADD COLUMN IF NOT EXISTS occurred_at_utc TIMESTAMPTZ;
ALTER TABLE silver.web_events ADD COLUMN IF NOT EXISTS source_row_id TEXT;
-- Menambahkan kolom silver_processed_at jika belum ada
ALTER TABLE silver.order_items 
ADD COLUMN IF NOT EXISTS silver_processed_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'utc');
ALTER TABLE silver.inventory_snapshots 
ADD COLUMN IF NOT EXISTS deterministic_key VARCHAR(255),
ADD COLUMN IF NOT EXISTS snapshot_timestamp_utc TIMESTAMPTZ;

-- Backfill
UPDATE silver.inventory_snapshots SET
  deterministic_key = inventory_id || '|' || product_id || '|' || store_id || '|' || snapshot_date_utc::text,
  snapshot_timestamp_utc = COALESCE(silver_processed_at, snapshot_date_utc::timestamptz, NOW())
WHERE deterministic_key IS NULL;

CREATE INDEX IF NOT EXISTS idx_inv_deterministic ON silver.inventory_snapshots(deterministic_key);
ALTER TABLE silver.campaign_spend 
ADD COLUMN IF NOT EXISTS deterministic_key VARCHAR(512),
ADD COLUMN IF NOT EXISTS silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS snapshot_timestamp_utc TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_campaign_deterministic ON silver.campaign_spend(deterministic_key);
-- === FIX SCHEMA PURE ===
ALTER TABLE silver.campaign_spend ADD COLUMN IF NOT EXISTS deterministic_key VARCHAR(512);
ALTER TABLE silver.web_events ADD COLUMN IF NOT EXISTS deterministic_key VARCHAR(512);
ALTER TABLE silver.web_events ADD COLUMN IF NOT EXISTS is_current BOOLEAN DEFAULT TRUE;
ALTER TABLE silver.web_events ADD COLUMN IF NOT EXISTS silver_processed_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE silver.web_events ADD COLUMN IF NOT EXISTS event_timestamp_utc TIMESTAMPTZ;
ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS order_date DATE;

-- ======================================================
-- 20. FIX ON CONFLICT - UNIQUE INDEX UNTUK BUILD_SILVER.PY
-- ======================================================
-- Ini bikin ON CONFLICT gak error lagi walaupun PK di-drop manual
CREATE UNIQUE INDEX IF NOT EXISTS uq_order_items_order_item_id ON silver.order_items (order_item_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_events_payment_id ON silver.payment_events (payment_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_events_event_id ON silver.payment_events (event_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_customers_customer_id ON silver.customers (customer_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_order_id ON silver.orders (order_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_products_product_id ON silver.products (product_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_stores_store_id ON silver.stores (store_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_web_events_event_id ON silver.web_events (event_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_inventory_id_date ON silver.inventory_snapshots (inventory_id, snapshot_date_utc);
CREATE UNIQUE INDEX IF NOT EXISTS uq_campaign_spend_pk ON silver.campaign_spend (campaign_id, spend_date_utc);

-- Fix quantity check biar gak block insert 0
ALTER TABLE silver.order_items DROP CONSTRAINT IF EXISTS order_items_quantity_check;
ALTER TABLE silver.order_items DROP CONSTRAINT IF EXISTS silver_order_items_quantity_check;
