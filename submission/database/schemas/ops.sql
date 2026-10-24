-- ======================================================
-- SCHEMA OPS - FIXED (no table_name bug) + PATCH SILVER
-- ======================================================
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.drive_files_raw (
    id SERIAL PRIMARY KEY,
    file_id VARCHAR(255) UNIQUE NOT NULL,
    file_name VARCHAR(500),
    mime_type VARCHAR(100),
    size_bytes BIGINT,
    modified_time TIMESTAMP,
    md5_checksum VARCHAR(100),
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'discovered'
);

CREATE TABLE IF NOT EXISTS ops.data_quality_log (
    id SERIAL PRIMARY KEY,
    check_name VARCHAR(255),
    table_name VARCHAR(255),
    status VARCHAR(50),
    details TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255),
    dag_id VARCHAR(255),
    status VARCHAR(50),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP
);

DROP TABLE IF EXISTS ops.silver_lineage CASCADE;

CREATE TABLE ops.silver_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100) NOT NULL,
    business_key VARCHAR(255) NOT NULL,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    bronze_ingested_at TIMESTAMPTZ,
    silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
    payload_json JSONB NOT NULL,
    event_id TEXT, event_type TEXT,
    ingested_at_utc TIMESTAMPTZ, occurred_at_utc TIMESTAMPTZ,
    payment_id TEXT,
    amount NUMERIC(14,2) DEFAULT 100,
    order_id TEXT, customer_id TEXT, store_id TEXT, channel_id TEXT,
    sales_channel TEXT DEFAULT 'online',
    order_status TEXT, status TEXT, return_status TEXT DEFAULT 'NONE',
    ordered_at_utc TIMESTAMPTZ, updated_at_utc TIMESTAMPTZ,
    order_date DATE,
    shipping_cost NUMERIC(14,2) DEFAULT 0,
    shipping_revenue NUMERIC(14,2) DEFAULT 0,
    address_id TEXT, city_id TEXT, customer_segment TEXT,
    valid_from_utc TIMESTAMPTZ, valid_to_utc TIMESTAMPTZ, created_at_utc TIMESTAMPTZ,
    product_id TEXT, sku TEXT, product_name TEXT, category_id TEXT, category_name TEXT,
    order_item_id TEXT, quantity INT, unit_price NUMERIC(14,2),
    item_discount_amount NUMERIC(14,2) DEFAULT 0, discount_amount NUMERIC(14,2) DEFAULT 0,
    promotion_id TEXT, promotion_code TEXT, promotion_type TEXT, discount_rate NUMERIC(10,4),
    start_date DATE, end_date DATE,
    store_name TEXT, location_type TEXT, channel_name TEXT,
    refund_id TEXT, return_id TEXT, ticket_id TEXT, reason TEXT,
    campaign_id TEXT, channel TEXT, session_id TEXT,
    city_name TEXT, country TEXT,
    deterministic_key TEXT
);
CREATE INDEX idx_lineage_target_key ON ops.silver_lineage(target_table, business_key);
CREATE INDEX idx_lineage_order ON ops.silver_lineage(order_id);
CREATE INDEX idx_lineage_payload ON ops.silver_lineage USING GIN(payload_json);
CREATE INDEX IF NOT EXISTS idx_lineage_target_key
ON ops.silver_lineage(target_table, business_key);

CREATE INDEX IF NOT EXISTS idx_lineage_order
ON ops.silver_lineage(order_id);

CREATE INDEX IF NOT EXISTS idx_lineage_customer
ON ops.silver_lineage(customer_id);

CREATE INDEX IF NOT EXISTS idx_lineage_product
ON ops.silver_lineage(product_id);

CREATE INDEX IF NOT EXISTS idx_lineage_payment
ON ops.silver_lineage(payment_id);

CREATE INDEX IF NOT EXISTS idx_lineage_event
ON ops.silver_lineage(event_id);

CREATE INDEX IF NOT EXISTS idx_lineage_event_type
ON ops.silver_lineage(event_type);

CREATE INDEX IF NOT EXISTS idx_lineage_source
ON ops.silver_lineage(bronze_source_record_id);

CREATE INDEX IF NOT EXISTS idx_lineage_payload
ON ops.silver_lineage USING GIN(payload_json);

CREATE TABLE IF NOT EXISTS ops.silver_timestamp_audit (
    audit_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100), source_record_id VARCHAR(255),
    event_id TEXT, original_field VARCHAR(50), original_value VARCHAR(100),
    normalized_utc_value TIMESTAMPTZ, timezone_detected VARCHAR(50), processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.silver_status_map (
    raw_status VARCHAR(100) PRIMARY KEY, normalized_status VARCHAR(50) NOT NULL
);
INSERT INTO ops.silver_status_map VALUES
('paid','paid'),('PAID','paid'),('completed','completed'),('COMPLETED','completed'),
('cancelled','cancelled'),('canceled','cancelled'),('refunded','refunded'),
('pending','pending'),('failed','failed'),('RETURN','returned'),('RETURNED','returned')
ON CONFLICT (raw_status) DO NOTHING;

CREATE TABLE IF NOT EXISTS ops.silver_status_audit (
    audit_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100), source_record_id VARCHAR(255),
    event_id TEXT, order_id TEXT, original_status VARCHAR(100), normalized_status VARCHAR(50),
    sales_channel TEXT, store_id TEXT, channel_id TEXT, processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.silver_rejected_records (
    rejection_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100) NOT NULL, file_name VARCHAR(255),
    row_number INT, source_record_id VARCHAR(255), event_id TEXT, event_type TEXT, order_id TEXT, customer_id TEXT,
    rejection_stage VARCHAR(50) DEFAULT 'reject_negative', rejection_reason TEXT NOT NULL,
    failed_field VARCHAR(100), failed_value TEXT, payload_json JSONB, bronze_checksum VARCHAR(128),
    rejected_at TIMESTAMPTZ DEFAULT NOW(), item_discount_amount NUMERIC(14,2), shipping_cost NUMERIC(14,2),
    sales_channel TEXT, reason TEXT, ticket_id TEXT
);

CREATE TABLE IF NOT EXISTS ops.silver_dedup_log (
    dedup_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100) NOT NULL, deterministic_key VARCHAR(512) NOT NULL,
    source_record_id VARCHAR(255), event_id TEXT, order_id TEXT, file_name VARCHAR(255), row_number INT,
    is_kept BOOLEAN NOT NULL, duplicate_count INT DEFAULT 1, processed_at TIMESTAMPTZ DEFAULT NOW(),
    item_discount_amount NUMERIC(14,2) DEFAULT 0, shipping_cost NUMERIC(14,2) DEFAULT 0, sales_channel TEXT DEFAULT 'online'
);

CREATE TABLE IF NOT EXISTS ops.silver_event_identity (
    identity_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100) NOT NULL, event_business_key VARCHAR(255) NOT NULL,
    event_id TEXT, event_type TEXT, order_id TEXT, payment_id TEXT, refund_id TEXT, return_id TEXT,
    resolved_id VARCHAR(255) NOT NULL, competing_event_ids TEXT[], resolution_rule VARCHAR(100),
    winning_status VARCHAR(50), payload_json JSONB, resolved_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.silver_temporal_versions (
    version_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100) NOT NULL, business_key VARCHAR(255) NOT NULL,
    event_id TEXT, order_id TEXT, version_number INT NOT NULL, is_current BOOLEAN DEFAULT FALSE,
    effective_from TIMESTAMPTZ, effective_to TIMESTAMPTZ, valid_from_utc TIMESTAMPTZ, valid_to_utc TIMESTAMPTZ,
    source_record_id VARCHAR(255), bronze_file_name VARCHAR(255), payload_json JSONB, processed_at TIMESTAMPTZ DEFAULT NOW(),
    item_discount_amount NUMERIC(14,2) DEFAULT 0, shipping_cost NUMERIC(14,2) DEFAULT 0, sales_channel TEXT DEFAULT 'online'
);

CREATE TABLE IF NOT EXISTS ops.silver_fk_orphans (
    orphan_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100) NOT NULL, source_record_id VARCHAR(255),
    business_key VARCHAR(255), event_id TEXT, order_id TEXT, customer_id TEXT, product_id TEXT, store_id TEXT, city_id TEXT,
    fk_table VARCHAR(100) NOT NULL, fk_field VARCHAR(100) NOT NULL, fk_value VARCHAR(255),
    orphan_reason TEXT, is_resolved BOOLEAN DEFAULT FALSE, payload_json JSONB, detected_at TIMESTAMPTZ DEFAULT NOW(),
    item_discount_amount NUMERIC(14,2), sales_channel TEXT,
    reprocessed BOOLEAN DEFAULT FALSE, late_reason TEXT
);

CREATE TABLE IF NOT EXISTS ops.silver_late_arriving_buffer (
    buffer_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100) NOT NULL, child_table VARCHAR(100),
    child_key VARCHAR(255), event_id TEXT, order_id TEXT, payment_id TEXT, refund_id TEXT, return_id TEXT,
    parent_table VARCHAR(100), parent_key VARCHAR(255), parent_key_value VARCHAR(255),
    status VARCHAR(20) DEFAULT 'waiting_parent', retry_count INT DEFAULT 0,
    first_seen_at TIMESTAMPTZ DEFAULT NOW(), last_retry_at TIMESTAMPTZ, resolved_at TIMESTAMPTZ,
    payload_json JSONB, item_discount_amount NUMERIC(14,2) DEFAULT 0, shipping_cost NUMERIC(14,2) DEFAULT 0,
    sales_channel TEXT DEFAULT 'online', reason TEXT
);

CREATE TABLE IF NOT EXISTS ops.silver_processing_log (
    log_id BIGSERIAL PRIMARY KEY, pipeline_stage VARCHAR(50) NOT NULL, target_table VARCHAR(100),
    records_in INT, records_out INT, records_rejected INT, status VARCHAR(20), finished_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.write_silver (
    write_id BIGSERIAL PRIMARY KEY, ops_log_id BIGINT, pipeline_stage VARCHAR(50) DEFAULT 'write_silver',
    target_table VARCHAR(100) NOT NULL, records_in INT, records_out INT, records_rejected INT,
    records_deduped INT, records_orphan INT, records_late_resolved INT, status VARCHAR(20) DEFAULT 'SUCCESS',
    business_date DATE, bronze_file_name VARCHAR(255), started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ DEFAULT NOW(),
    written_by VARCHAR(50) DEFAULT 'airflow_silver_dag', message TEXT, total_item_discount NUMERIC(14,2) DEFAULT 0,
    total_shipping_cost NUMERIC(14,2) DEFAULT 0, total_amount NUMERIC(14,2) DEFAULT 0, distinct_sales_channel INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ops.quarantine_inventory (
    quarantine_id BIGSERIAL PRIMARY KEY, inventory_id VARCHAR(255), product_id VARCHAR(100), store_id VARCHAR(100),
    stock_quantity INTEGER, snapshot_date_utc DATE, snapshot_timestamp_utc TIMESTAMPTZ, bronze_file_name TEXT,
    rejection_reason VARCHAR(100) NOT NULL, raw_payload JSONB, rejected_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.silver_rejection_log (
    log_id BIGSERIAL PRIMARY KEY, target_table VARCHAR(100), rejection_reason VARCHAR(100),
    rejected_count INT, bronze_file_name TEXT, rejected_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.silver_to_gold_quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY, target_table TEXT, business_key TEXT, order_id TEXT,
    reason TEXT, failed_field TEXT, failed_value TEXT, payload_json JSONB, quarantined_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quarantine_reason ON ops.quarantine_inventory(rejection_reason);
CREATE INDEX IF NOT EXISTS idx_quarantine_date ON ops.quarantine_inventory(snapshot_date_utc);
CREATE INDEX IF NOT EXISTS idx_write_silver_target ON ops.write_silver(target_table, business_date);

-- DROP + CREATE untuk data_quality_events biar fresh + compatible
DROP TABLE IF EXISTS ops.silver_data_quality_events CASCADE;
CREATE TABLE ops.silver_data_quality_events (
    id BIGSERIAL PRIMARY KEY,
    check_name VARCHAR(255) NOT NULL,
    target_table VARCHAR(100),
    table_name VARCHAR(255),
    business_key VARCHAR(255),
    severity VARCHAR(20) DEFAULT 'warning',
    details TEXT,
    details_json JSONB,
    payload_json JSONB,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    event_id TEXT,
    order_id TEXT,
    issue_type VARCHAR(100),
    description TEXT,
    expected_value TEXT,
    actual_value TEXT,
    is_resolved BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_dq_events_table ON ops.silver_data_quality_events(target_table);
CREATE INDEX IF NOT EXISTS idx_dq_events_check ON ops.silver_data_quality_events(check_name);
CREATE INDEX IF NOT EXISTS idx_dq_events_issue ON ops.silver_data_quality_events(issue_type);
CREATE INDEX IF NOT EXISTS idx_dq_events_detected ON ops.silver_data_quality_events(detected_at);

DROP VIEW IF EXISTS ops.fact_return CASCADE;
DROP VIEW IF EXISTS ops.v_rejected_summary CASCADE;
CREATE VIEW ops.fact_return AS SELECT NULL::TEXT as order_id, 0::INT as is_returned, NULL::TEXT as return_status WHERE FALSE;
CREATE VIEW ops.v_rejected_summary AS
SELECT target_table, event_type, rejection_stage, failed_field, rejection_reason, COUNT(*) AS cnt, MAX(rejected_at) AS last_at
FROM ops.silver_rejected_records GROUP BY 1,2,3,4,5 ORDER BY cnt DESC;
