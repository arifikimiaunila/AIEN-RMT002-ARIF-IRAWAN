-- ==========================================
-- SCHEMA DEFINITION
-- ==========================================
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id TEXT PRIMARY KEY,
    dag_id TEXT,
    status TEXT,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

-- =========================================================================
-- DDL: OPS.SILVER_ANOMALY_LOG
-- =========================================================================

CREATE TABLE IF NOT EXISTS ops.silver_anomaly_log (
    anomaly_id     SERIAL PRIMARY KEY,
    check_name     VARCHAR(100),
    target_table   VARCHAR(100),
    table_name     VARCHAR(100),
    issue_type     VARCHAR(100),
    description    TEXT,
    details_json   JSONB,
    recorded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.silver_data_quality_events (
    event_id BIGSERIAL PRIMARY KEY,
    check_name TEXT,
    target_table TEXT,
    severity TEXT,
    issue_type TEXT,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ==========================================
-- 1. INFRASTRUCTURE & PIPELINE ORCHESTRATION
-- ==========================================

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

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255),
    dag_id VARCHAR(255),
    status VARCHAR(50),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.silver_processing_log (
    log_id BIGSERIAL PRIMARY KEY,
    pipeline_stage VARCHAR(50) NOT NULL,
    target_table VARCHAR(100),
    records_in INT,
    records_out INT,
    records_rejected INT,
    status VARCHAR(20),
    finished_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.write_silver (
    write_id BIGSERIAL PRIMARY KEY,
    ops_log_id BIGINT,
    pipeline_stage VARCHAR(50) DEFAULT 'write_silver',
    target_table VARCHAR(100) NOT NULL,
    records_in INT,
    records_out INT,
    records_rejected INT,
    records_deduped INT,
    records_orphan INT,
    records_late_resolved INT,
    status VARCHAR(20) DEFAULT 'SUCCESS',
    business_date DATE,
    bronze_file_name VARCHAR(255),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ DEFAULT NOW(),
    written_by VARCHAR(50) DEFAULT 'airflow_silver_dag',
    message TEXT,
    total_item_discount NUMERIC(14,2) DEFAULT 0,
    total_shipping_cost NUMERIC(14,2) DEFAULT 0,
    distinct_sales_channel INT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_write_silver_target ON ops.write_silver(target_table, business_date);

-- ==========================================
-- 2. LINEAGE & AUDITING LOGS
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.silver_lineage (
    lineage_id               BIGSERIAL PRIMARY KEY,
    deterministic_key        TEXT NOT NULL,
    business_key             VARCHAR(255),
    order_id                 TEXT,
    customer_id              TEXT,
    product_id               TEXT,
    payment_id               TEXT,
    event_id                 TEXT,
    event_type               TEXT,
    city_id                  TEXT,
    store_id                 TEXT,
    channel_id               TEXT,
    address_id               TEXT,
    order_item_id            TEXT,
    promotion_id             TEXT,
    campaign_id              TEXT,
    refund_id                TEXT,
    return_id                TEXT,
    ticket_id                TEXT,
    amount                   NUMERIC(14,2),
    shipping_revenue         NUMERIC(14,2) DEFAULT 0.00, -- Ditambahkan untuk mengakomodasi kolom shipping_revenue
    quantity                 NUMERIC(14,2),
    order_status             TEXT,
    payment_status           TEXT,
    status                   TEXT,
    source_table             TEXT,
    target_table             VARCHAR(100) NOT NULL,
    bronze_file_name         VARCHAR(255),
    bronze_row_number        INT,
    bronze_source_record_id  VARCHAR(255),
    bronze_checksum          VARCHAR(128),
    bronze_ingested_at       TIMESTAMPTZ,
    file_name                TEXT,
    payload_json             JSONB NOT NULL,
    job_name                 TEXT,
    marquez_run_id           TEXT,
    recorded_at              TIMESTAMPTZ DEFAULT NOW(),
    marquez_facets           JSONB,
    CONSTRAINT uq_lineage_detkey UNIQUE (deterministic_key)
);
CREATE INDEX IF NOT EXISTS idx_silver_lineage_business_key ON ops.silver_lineage(business_key);
CREATE INDEX IF NOT EXISTS idx_silver_lineage_target_table ON ops.silver_lineage(target_table);
CREATE INDEX IF NOT EXISTS idx_silver_lineage_order_id ON ops.silver_lineage(order_id);

CREATE TABLE IF NOT EXISTS ops.silver_timestamp_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100),
    source_record_id VARCHAR(255),
    event_id TEXT,
    original_field VARCHAR(50),
    original_value VARCHAR(100),
    normalized_utc_value TIMESTAMPTZ,
    timezone_detected VARCHAR(50),
    processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.silver_status_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100),
    source_record_id VARCHAR(255),
    event_id TEXT,
    order_id TEXT,
    original_status VARCHAR(100),
    normalized_status VARCHAR(50),
    sales_channel TEXT,
    store_id TEXT,
    channel_id TEXT,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.silver_dedup_log (
    dedup_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100) NOT NULL,
    deterministic_key VARCHAR(512) NOT NULL,
    source_record_id VARCHAR(255),
    payment_id TEXT,
    event_id TEXT,
    order_id TEXT,
    address_id TEXT,
    city_id TEXT, 
    product_id TEXT, 
    promotion_id TEXT,
    channel_id TEXT,
    store_id TEXT,
    campaign_id TEXT, 
    file_name VARCHAR(255),
    row_number INT,
    is_kept BOOLEAN NOT NULL,
    duplicate_count INT DEFAULT 1,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    item_discount_amount NUMERIC(14,2) DEFAULT 0,
    shipping_cost NUMERIC(14,2) DEFAULT 0,
    sales_channel TEXT DEFAULT 'online'
);

-- ==========================================
-- 3. REFERENCE & STATUS MAP
-- ==========================================

-- =========================================================================
-- DDL & DML: OPS.SILVER_STATUS_MAP
-- =========================================================================

CREATE TABLE IF NOT EXISTS ops.silver_status_map (
    raw_status VARCHAR(100) PRIMARY KEY,
    normalized_status VARCHAR(50) NOT NULL
);

-- Mapping status lengkap dengan variasi kapitalisasi
INSERT INTO ops.silver_status_map (raw_status, normalized_status) VALUES
    -- 1. Pending / Early Stage
    ('pending', 'pending'), ('PENDING', 'pending'), ('Pending', 'pending'),
    ('placed', 'pending'), ('PLACED', 'pending'), ('Placed', 'pending'),
    ('confirmed', 'pending'), ('CONFIRMED', 'pending'), ('Confirmed', 'pending'),

    -- 2. Completed / Fulfilled
    ('completed', 'completed'), ('COMPLETED', 'completed'), ('Completed', 'completed'),
    ('fulfilled', 'completed'), ('FULFILLED', 'completed'), ('Fulfilled', 'completed'),
    ('paid', 'completed'), ('PAID', 'completed'), ('Paid', 'completed'),

    -- 3. Cancelled
    ('cancelled', 'cancelled'), ('CANCELLED', 'cancelled'), ('Cancelled', 'cancelled'),
    ('canceled', 'cancelled'), ('CANCELED', 'cancelled'), ('Canceled', 'cancelled'),

    -- 4. Returned
    ('returned', 'returned'), ('RETURNED', 'returned'), ('Returned', 'returned'),
    ('return', 'returned'), ('RETURN', 'returned'), ('Return', 'returned'),

    -- 5. Refunded
    ('refunded', 'refunded'), ('REFUNDED', 'refunded'), ('Refunded', 'refunded'),

    -- 6. Failed
    ('failed', 'failed'), ('FAILED', 'failed'), ('Failed', 'failed')
ON CONFLICT (raw_status) DO UPDATE 
SET normalized_status = EXCLUDED.normalized_status;

-- ==========================================
-- 4. STATE MANAGEMENT & IDENTITY RESOLUTION
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.silver_event_identity (
    identity_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100) NOT NULL,
    event_business_key VARCHAR(255) NOT NULL,
    event_id TEXT,
    event_type TEXT,
    order_id TEXT,
    payment_id TEXT,
    refund_id TEXT,
    return_id TEXT,
    resolved_id VARCHAR(255) NOT NULL,
    competing_event_ids TEXT[],
    resolution_rule VARCHAR(100),
    winning_status VARCHAR(50),
    payload_json JSONB,
    resolved_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.silver_temporal_versions (
    version_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100) NOT NULL,
    business_key VARCHAR(255) NOT NULL,
    event_id TEXT,
    order_id TEXT,
    version_number INT NOT NULL,
    is_current BOOLEAN DEFAULT FALSE,
    effective_from TIMESTAMPTZ,
    effective_to TIMESTAMPTZ,
    valid_from_utc TIMESTAMPTZ,
    valid_to_utc TIMESTAMPTZ,
    source_record_id VARCHAR(255),
    bronze_file_name VARCHAR(255),
    payload_json JSONB,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    item_discount_amount NUMERIC(14,2) DEFAULT 0,
    shipping_cost NUMERIC(14,2) DEFAULT 0,
    sales_channel TEXT DEFAULT 'online'
);

-- ==========================================
-- 5. ERROR HANDLING, ORPHANS & BUFFERS
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.silver_fk_orphans (
    orphan_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100) NOT NULL,
    source_record_id VARCHAR(255),
    business_key VARCHAR(255),
    event_id TEXT,
    order_id TEXT,
    customer_id TEXT,
    product_id TEXT,
    store_id TEXT,
    city_id TEXT,
    fk_table VARCHAR(100) NOT NULL,
    fk_field VARCHAR(100) NOT NULL,
    fk_value VARCHAR(255),
    orphan_reason TEXT,
    is_resolved BOOLEAN DEFAULT FALSE,
    payload_json JSONB,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    item_discount_amount NUMERIC(14,2),
    sales_channel TEXT,
    reprocessed BOOLEAN DEFAULT FALSE,
    late_reason TEXT
);

CREATE TABLE IF NOT EXISTS ops.silver_late_arriving_buffer (
    buffer_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100) NOT NULL,
    child_table VARCHAR(100),
    child_key VARCHAR(255),
    event_id TEXT,
    order_id TEXT,
    payment_id TEXT ,
    refund_id TEXT,
    return_id TEXT,
    parent_table VARCHAR(100),
    parent_key VARCHAR(255),
    parent_key_value VARCHAR(255),
    status VARCHAR(20) DEFAULT 'waiting_parent',
    retry_count INT DEFAULT 0,
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_retry_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    payload_json JSONB,
    item_discount_amount NUMERIC(14,2) DEFAULT 0,
    shipping_cost NUMERIC(14,2) DEFAULT 0,
    sales_channel TEXT DEFAULT 'online',
    reason TEXT
);

-- ==========================================
-- 6. REJECTIONS & QUARANTINE LOGS
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.silver_rejected_records (
    rejection_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100) NOT NULL,
    file_name VARCHAR(255),
    row_number INT,
    payment_id TEXT,
    source_record_id VARCHAR(255),
    event_id TEXT,
    event_type TEXT,
    order_id TEXT,
    customer_id TEXT,
    promotion_id TEXT,
    rejection_stage VARCHAR(50) DEFAULT 'reject_negative',
    rejection_reason TEXT NOT NULL,
    failed_field VARCHAR(100),
    failed_value TEXT,
    payload_json JSONB,
    bronze_checksum VARCHAR(128),
    rejected_at TIMESTAMPTZ DEFAULT NOW(),
    item_discount_amount NUMERIC(14,2),
    shipping_cost NUMERIC(14,2),
    sales_channel TEXT,
    reason TEXT,
    ticket_id TEXT
);

CREATE TABLE IF NOT EXISTS ops.quarantine_inventory (
    quarantine_id BIGSERIAL PRIMARY KEY,
    inventory_id VARCHAR(255),
    product_id VARCHAR(100),
    store_id VARCHAR(100),
    stock_quantity INTEGER,
    snapshot_date_utc DATE,
    snapshot_timestamp_utc TIMESTAMPTZ,
    bronze_file_name TEXT,
    rejection_reason VARCHAR(100) NOT NULL,
    raw_payload JSONB,
    rejected_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_quarantine_reason ON ops.quarantine_inventory(rejection_reason);
CREATE INDEX IF NOT EXISTS idx_quarantine_date ON ops.quarantine_inventory(snapshot_date_utc);

CREATE TABLE IF NOT EXISTS ops.silver_rejection_log (
    log_id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(100),
    rejection_reason VARCHAR(100),
    rejected_count INT,
    bronze_file_name TEXT,
    rejected_at TIMESTAMPTZ DEFAULT NOW()
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

-- ==========================================
-- 7. DATA QUALITY LOGS & EVENTS
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.data_quality_log (
    id SERIAL PRIMARY KEY,
    check_name VARCHAR(255),
    table_name VARCHAR(255),
    status VARCHAR(50),
    details TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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

-- ==========================================
-- 8. OPERATIONAL VIEWS
-- ==========================================

DROP VIEW IF EXISTS ops.fact_return CASCADE;
DROP VIEW IF EXISTS ops.v_rejected_summary CASCADE;

CREATE VIEW ops.fact_return AS 
SELECT 
    NULL::TEXT AS order_id, 
    0::INT AS is_returned, 
    NULL::TEXT AS event_type 
WHERE FALSE;

CREATE VIEW ops.v_rejected_summary AS
SELECT 
    target_table, 
    event_type, 
    rejection_stage, 
    failed_field, 
    rejection_reason, 
    COUNT(*) AS cnt, 
    MAX(rejected_at) AS last_at
FROM ops.silver_rejected_records 
GROUP BY 1, 2, 3, 4, 5 
ORDER BY cnt DESC;
