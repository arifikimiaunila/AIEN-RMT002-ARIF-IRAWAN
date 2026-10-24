CREATE SCHEMA IF NOT EXISTS bronze;

CREATE OR REPLACE FUNCTION bronze.immutable_timestamptz(t TEXT)
RETURNS TIMESTAMPTZ AS $$ SELECT NULLIF(t,'')::TIMESTAMPTZ $$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION bronze.immutable_date(t TEXT)
RETURNS DATE AS $$ SELECT NULLIF(t,'')::DATE $$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION bronze.immutable_numeric(t TEXT)
RETURNS NUMERIC AS $$ SELECT NULLIF(t,'')::NUMERIC $$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION bronze.immutable_int(t TEXT)
RETURNS INT AS $$ SELECT NULLIF(t,'')::INT $$ LANGUAGE sql IMMUTABLE;

CREATE TABLE IF NOT EXISTS bronze.payment_events (
    id BIGSERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    row_number INT NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    event_id TEXT GENERATED ALWAYS AS (payload->>'event_id') STORED,
    event_type TEXT GENERATED ALWAYS AS (payload->>'event_type') STORED,
    ingested_at_utc TIMESTAMPTZ GENERATED ALWAYS AS (bronze.immutable_timestamptz(payload->>'ingested_at_utc')) STORED,
    occurred_at_utc TIMESTAMPTZ GENERATED ALWAYS AS (bronze.immutable_timestamptz(payload->>'occurred_at_utc')) STORED,
    payment_id TEXT GENERATED ALWAYS AS (payload->>'payment_id') STORED,
    order_id TEXT GENERATED ALWAYS AS (payload->>'order_id') STORED,
    amount NUMERIC(14,2) GENERATED ALWAYS AS (bronze.immutable_numeric(payload->>'amount')) STORED,
    payment_amount NUMERIC(14,2) GENERATED ALWAYS AS (COALESCE(bronze.immutable_numeric(payload->>'payment_amount'), bronze.immutable_numeric(payload->>'amount'))) STORED,
    payment_status TEXT GENERATED ALWAYS AS (payload->>'payment_status') STORED,
    payment_method TEXT GENERATED ALWAYS AS (payload->>'payment_method') STORED,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.ingested_data_events (
    id BIGSERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    row_number INT NOT NULL,
    source_record_id VARCHAR(100) NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    event_id TEXT GENERATED ALWAYS AS (payload->>'event_id') STORED,
    event_type TEXT GENERATED ALWAYS AS (payload->>'event_type') STORED,
    ingested_at_utc TIMESTAMPTZ GENERATED ALWAYS AS (bronze.immutable_timestamptz(payload->>'ingested_at_utc')) STORED,
    occurred_at_utc TIMESTAMPTZ GENERATED ALWAYS AS (bronze.immutable_timestamptz(payload->>'occurred_at_utc')) STORED,
    amount NUMERIC(14,2) GENERATED ALWAYS AS (bronze.immutable_numeric(payload->>'amount')) STORED,
    order_id TEXT GENERATED ALWAYS AS (payload->>'order_id') STORED,
    payment_amount NUMERIC(14,2) GENERATED ALWAYS AS (
        COALESCE(
            bronze.jsonb_to_numeric(payload, 'payment_amount'),
            bronze.jsonb_to_numeric(payload, 'amount'),
            0
        )
    ) STORED,
    payment_id TEXT GENERATED ALWAYS AS (payload->>'payment_id') STORED,
    refund_id TEXT GENERATED ALWAYS AS (payload->>'refund_id') STORED,
    return_id TEXT GENERATED ALWAYS AS (payload->>'return_id') STORED,
    customer_id TEXT GENERATED ALWAYS AS (payload->>'customer_id') STORED,
    reason TEXT GENERATED ALWAYS AS (payload->>'reason') STORED,
    ticket_id TEXT GENERATED ALWAYS AS (payload->>'ticket_id') STORED,
    campaign_id TEXT GENERATED ALWAYS AS (payload->>'campaign_id') STORED,
    channel TEXT GENERATED ALWAYS AS (payload->>'channel') STORED,
    session_id TEXT GENERATED ALWAYS AS (payload->>'session_id') STORED,
    address_id TEXT GENERATED ALWAYS AS (payload->>'address_id') STORED,
    city_id TEXT GENERATED ALWAYS AS (payload->>'city_id') STORED,
    source_row_id TEXT GENERATED ALWAYS AS (payload->>'source_row_id') STORED,
    valid_from_utc TIMESTAMPTZ GENERATED ALWAYS AS (bronze.immutable_timestamptz(payload->>'valid_from_utc')) STORED,
    valid_to_utc TIMESTAMPTZ GENERATED ALWAYS AS (bronze.immutable_timestamptz(payload->>'valid_to_utc')) STORED,
    customer_segment TEXT GENERATED ALWAYS AS (payload->>'customer_segment') STORED,
    created_at_utc TIMESTAMPTZ GENERATED ALWAYS AS (bronze.immutable_timestamptz(payload->>'created_at_utc')) STORED,
    item_discount_amount NUMERIC(14,2) GENERATED ALWAYS AS (COALESCE(bronze.immutable_numeric(payload->>'item_discount_amount'), bronze.immutable_numeric(payload->>'discount_amount'), 0)) STORED,
    order_item_id TEXT GENERATED ALWAYS AS (payload->>'order_item_id') STORED,
    product_id TEXT GENERATED ALWAYS AS (payload->>'product_id') STORED,
    quantity INT GENERATED ALWAYS AS (bronze.immutable_int(payload->>'quantity')) STORED,
    unit_price NUMERIC(14,2) GENERATED ALWAYS AS (bronze.immutable_numeric(payload->>'unit_price')) STORED,
    discount_amount NUMERIC(14,2) GENERATED ALWAYS AS (bronze.immutable_numeric(payload->>'discount_amount')) STORED,
    promotion_id TEXT GENERATED ALWAYS AS (payload->>'promotion_id') STORED,
    ordered_at_utc TIMESTAMPTZ GENERATED ALWAYS AS (bronze.immutable_timestamptz(payload->>'ordered_at_utc')) STORED,
    sales_channel TEXT GENERATED ALWAYS AS (COALESCE(payload->>'sales_channel', payload->>'channel', payload->>'channel_id', 'online')) STORED,
    shipping_revenue NUMERIC(14,2) GENERATED ALWAYS AS (bronze.immutable_numeric(payload->>'shipping_revenue')) STORED,
    shipping_cost NUMERIC(14,2) GENERATED ALWAYS AS (COALESCE(bronze.immutable_numeric(payload->>'shipping_cost'), bronze.immutable_numeric(payload->>'shipping_revenue'), 0)) STORED,
    status TEXT GENERATED ALWAYS AS (payload->>'status') STORED,
    store_id TEXT GENERATED ALWAYS AS (payload->>'store_id') STORED,
    updated_at_utc TIMESTAMPTZ GENERATED ALWAYS AS (bronze.immutable_timestamptz(payload->>'updated_at_utc')) STORED,
    category_id TEXT GENERATED ALWAYS AS (payload->>'category_id') STORED,
    category_name TEXT GENERATED ALWAYS AS (payload->>'category_name') STORED,
    product_name TEXT GENERATED ALWAYS AS (payload->>'product_name') STORED,
    sku TEXT GENERATED ALWAYS AS (payload->>'sku') STORED,
    discount_rate NUMERIC(10,4) GENERATED ALWAYS AS (bronze.immutable_numeric(payload->>'discount_rate')) STORED,
    end_date DATE GENERATED ALWAYS AS (bronze.immutable_date(payload->>'end_date')) STORED,
    promotion_code TEXT GENERATED ALWAYS AS (payload->>'promotion_code') STORED,
    promotion_type TEXT GENERATED ALWAYS AS (payload->>'promotion_type') STORED,
    start_date DATE GENERATED ALWAYS AS (bronze.immutable_date(payload->>'start_date')) STORED,
    channel_id TEXT GENERATED ALWAYS AS (payload->>'channel_id') STORED,
    channel_name TEXT GENERATED ALWAYS AS (payload->>'channel_name') STORED,
    location_type TEXT GENERATED ALWAYS AS (payload->>'location_type') STORED,
    store_name TEXT GENERATED ALWAYS AS (payload->>'store_name') STORED,
    city_name TEXT GENERATED ALWAYS AS (payload->>'city_name') STORED,
    country TEXT GENERATED ALWAYS AS (payload->>'country') STORED,
    order_status TEXT GENERATED ALWAYS AS (COALESCE(payload->>'order_status', payload->>'status')) STORED,
    return_status TEXT GENERATED ALWAYS AS (COALESCE(payload->>'return_status','NONE')) STORED,
    subtotal NUMERIC(14,2) GENERATED ALWAYS AS (bronze.immutable_numeric(payload->>'subtotal')) STORED,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ingested_file_name ON bronze.ingested_data_events(file_name);
CREATE INDEX IF NOT EXISTS idx_ingested_source_id ON bronze.ingested_data_events(source_record_id);
CREATE INDEX IF NOT EXISTS idx_ingested_event_type ON bronze.ingested_data_events(event_type);
CREATE INDEX IF NOT EXISTS idx_ingested_order_id ON bronze.ingested_data_events(order_id);
CREATE INDEX IF NOT EXISTS idx_ingested_payload ON bronze.ingested_data_events USING gin (payload);
CREATE INDEX IF NOT EXISTS idx_payment_source_id ON bronze.payment_events(source_record_id);
CREATE INDEX IF NOT EXISTS idx_payment_payload ON bronze.payment_events USING gin (payload);

CREATE OR REPLACE FUNCTION bronze.prevent_ingested_data_modification() RETURNS TRIGGER AS $$
BEGIN
  IF (TG_OP='UPDATE') THEN RAISE EXCEPTION 'immutable'; 
  ELSIF (TG_OP='DELETE') THEN RAISE EXCEPTION 'immutable'; 
  END IF; RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS enforce_immutable_ingested_data ON bronze.ingested_data_events;
CREATE TRIGGER enforce_immutable_ingested_data BEFORE UPDATE OR DELETE ON bronze.ingested_data_events FOR EACH ROW EXECUTE FUNCTION bronze.prevent_ingested_data_modification();

DROP TRIGGER IF EXISTS enforce_immutable_payment ON bronze.payment_events;
CREATE TRIGGER enforce_immutable_payment BEFORE UPDATE OR DELETE ON bronze.payment_events FOR EACH ROW EXECUTE FUNCTION bronze.prevent_ingested_data_modification();
