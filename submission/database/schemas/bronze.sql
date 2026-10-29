CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.raw_data CASCADE;

CREATE TABLE bronze.raw_data (
    bronze_id BIGSERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    row_number INT NOT NULL,
    source_system TEXT DEFAULT 'retail_generator',
    processing_status VARCHAR(20) DEFAULT 'NEW',
    error_message TEXT,
    payload JSONB NOT NULL,
    product_id TEXT,
    payment_id TEXT,
    store_id TEXT,
    snapshot_date TEXT,
    channel_id TEXT,
    channel_name TEXT,
    stock_on_hand INTEGER,
    stock_reserved INTEGER,
    unit_cost NUMERIC(14,2),
    source_record_id TEXT,
    customer_id TEXT,
    discount_amount INT,
    customer_segment TEXT,
    source_row_id TEXT,
    order_id TEXT,
    order_item_id TEXT,
    sales_channel TEXT,
    session_id TEXT,
    unit_price NUMERIC(12,2),
    checksum VARCHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_from_ts TIMESTAMPTZ,
    valid_to_ts TIMESTAMPTZ,
    ordered_to_ts TIMESTAMPTZ,
    address_id TEXT,
    category_id TEXT,
    category_name TEXT,
    city_id TEXT,
    product_name TEXT,
    quantity NUMERIC(14,2),
    sku TEXT,
    discount_rate NUMERIC(14,2),
    end_date DATE,
    promotion_code TEXT,
    promotion_id TEXT,
    promotion_type TEXT,
    start_date DATE,
    valid_from_utc TEXT,
    valid_to_utc TEXT,
    occured_at_utc TEXT,
    refund_id TEXT,
    amount NUMERIC(14,2),
    event_id TEXT,
    campaign_id TEXT,
    channel TEXT,
    event_type TEXT DEFAULT 'NONE',
    shipping_revenue NUMERIC(12,2),
    status TEXT,
    return_id TEXT,
    ingested_at_utc TEXT,
    occurred_at_utc TEXT,
    location_type TEXT,
    city_name TEXT,
    country TEXT,
    store_name TEXT,
    spend_date DATE,
    spend_amount NUMERIC(14,2),
    occurred_ts TIMESTAMPTZ,
    reason TEXT,
    source_ingested_ts TIMESTAMPTZ,
    target_table TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Trigger yang ngikutin image STABLE, pakai payload->> asli
CREATE OR REPLACE FUNCTION bronze.fill_raw_data() 
RETURNS TRIGGER 
LANGUAGE plpgsql AS $$
BEGIN
  -- 1. Identifikasi & Atribut Dasar Produk/Toko/Channel
  NEW.product_id := COALESCE(NEW.product_id, NEW.payload->>'product_id', NEW.payload #>> '{payload,product_id}');
  NEW.store_id := COALESCE(NEW.store_id, NEW.payload->>'store_id', NEW.payload #>> '{payload,store_id}');
  NEW.snapshot_date := COALESCE(NEW.snapshot_date, NEW.payload->>'snapshot_date', NEW.payload #>> '{payload,snapshot_date}');
  NEW.channel_id := COALESCE(NEW.channel_id, NEW.payload->>'channel_id', NEW.payload #>> '{payload,channel_id}');
  NEW.channel_name := COALESCE(NEW.channel_name, NEW.payload->>'channel_name', NEW.payload #>> '{payload,channel_name}');
  
  -- 2. Metrik Stok & Biaya
  NEW.stock_on_hand := NULLIF(COALESCE(NEW.stock_on_hand::text, NEW.payload->>'stock_on_hand', NEW.payload #>> '{payload,stock_on_hand}'), '')::INT;
  NEW.stock_reserved := NULLIF(COALESCE(NEW.stock_reserved::text, NEW.payload->>'stock_reserved', NEW.payload #>> '{payload,stock_reserved}'), '')::INT;
  NEW.unit_cost := NULLIF(COALESCE(NEW.unit_cost::text, NEW.payload->>'unit_cost', NEW.payload #>> '{payload,unit_cost}'), '')::NUMERIC;
  
  -- 3. Informasi Pelanggan & Transaksi
  NEW.customer_id := COALESCE(NEW.customer_id, NEW.payload->>'customer_id', NEW.payload #>> '{payload,customer_id}');
  NEW.discount_amount := NULLIF(COALESCE(NEW.discount_amount::text, NEW.payload->>'discount_amount', NEW.payload #>> '{payload,discount_amount}'), '')::INT;
  NEW.customer_segment := COALESCE(NEW.customer_segment, NEW.payload->>'customer_segment', NEW.payload #>> '{payload,customer_segment}');
  NEW.source_row_id := COALESCE(NEW.source_row_id, NEW.payload->>'source_row_id', NEW.payload #>> '{payload,source_row_id}');
  
  NEW.order_id := COALESCE(NEW.order_id, NEW.payload->>'order_id', NEW.payload #>> '{payload,order_id}');
  NEW.order_item_id := COALESCE(NEW.order_item_id, NEW.payload->>'order_item_id', NEW.payload #>> '{payload,order_item_id}');
  NEW.sales_channel := COALESCE(NEW.sales_channel, NEW.payload->>'sales_channel', NEW.payload #>> '{payload,sales_channel}');
  NEW.session_id := COALESCE(NEW.session_id, NEW.payload->>'session_id', NEW.payload #>> '{payload,session_id}');
  NEW.unit_price := NULLIF(COALESCE(NEW.unit_price::text, NEW.payload->>'unit_price', NEW.payload #>> '{payload,unit_price}'), '')::NUMERIC;
  
  -- 4. Alamat, Kategori, & Referensi Produk
  NEW.address_id := COALESCE(NEW.address_id, NEW.payload->>'address_id', NEW.payload #>> '{payload,address_id}');
  NEW.category_id := COALESCE(NEW.category_id, NEW.payload->>'category_id', NEW.payload #>> '{payload,category_id}');
  NEW.category_name := COALESCE(NEW.category_name, NEW.payload->>'category_name', NEW.payload #>> '{payload,category_name}');
  NEW.city_id := COALESCE(NEW.city_id, NEW.payload->>'city_id', NEW.payload #>> '{payload,city_id}');
  NEW.product_name := COALESCE(NEW.product_name, NEW.payload->>'product_name', NEW.payload #>> '{payload,product_name}');
  NEW.sku := COALESCE(NEW.sku, NEW.payload->>'sku', NEW.payload #>> '{payload,sku}');
  
  -- 5. Diskon, Tanggal, & Promosi
  NEW.discount_rate := NULLIF(COALESCE(NEW.discount_rate::text, NEW.payload->>'discount_rate', NEW.payload #>> '{payload,discount_rate}'), '')::NUMERIC;
  NEW.end_date := NULLIF(COALESCE(NEW.end_date::text, NEW.payload->>'end_date', NEW.payload #>> '{payload,end_date}'), '')::DATE;
  
  NEW.promotion_code := COALESCE(NEW.promotion_code, NEW.payload->>'promotion_code', NEW.payload #>> '{payload,promotion_code}');
  NEW.promotion_id := COALESCE(NEW.promotion_id, NEW.payload->>'promotion_id', NEW.payload #>> '{payload,promotion_id}', NEW.payload->>'id');
  NEW.promotion_type := COALESCE(NEW.promotion_type, NEW.payload->>'promotion_type', NEW.payload #>> '{payload,promotion_type}');
  NEW.start_date := NULLIF(COALESCE(NEW.start_date::text, NEW.payload->>'start_date', NEW.payload #>> '{payload,start_date}'), '')::DATE;
  
  NEW.valid_from_utc := COALESCE(NEW.valid_from_utc, NEW.payload->>'valid_from_utc', NEW.payload #>> '{payload,valid_from_utc}');
  NEW.valid_to_utc := COALESCE(NEW.valid_to_utc, NEW.payload->>'valid_to_utc', NEW.payload #>> '{payload,valid_to_utc}');
  NEW.occured_at_utc := COALESCE(NEW.occured_at_utc, NEW.payload->>'occured_at_utc', NEW.payload #>> '{payload,occured_at_utc}');
  
  -- 6. Event Khusus (Refund, Payment, Return) - FIX cross payment_id <-> event_id
  NEW.refund_id := COALESCE(NEW.refund_id, NEW.payload->>'refund_id', NEW.payload #>> '{payload,refund_id}', NEW.payload->>'id');
  NEW.payment_id := COALESCE(NEW.payment_id, NEW.payload->>'payment_id', NEW.payload #>> '{payload,payment_id}', NEW.payload->>'id', NEW.payload->>'event_id', NEW.payload #>> '{payload,event_id}');
  NEW.return_id := COALESCE(NEW.return_id, NEW.payload->>'return_id', NEW.payload #>> '{payload,return_id}', NEW.payload->>'id');
  
  NEW.amount := NULLIF(COALESCE(NEW.amount::text, NEW.payload->>'amount', NEW.payload #>> '{payload,amount}'), '')::NUMERIC;
  NEW.event_id := COALESCE(NEW.event_id, NEW.payload->>'event_id', NEW.payload #>> '{payload,event_id}', NEW.payload->>'payment_id', NEW.payload #>> '{payload,payment_id}', NEW.payload->>'id');
  NEW.campaign_id := COALESCE(NEW.campaign_id, NEW.payload->>'campaign_id', NEW.payload #>> '{payload,campaign_id}');
  NEW.channel := COALESCE(NEW.channel, NEW.payload->>'channel', NEW.payload #>> '{payload,channel}');
  NEW.event_type := COALESCE(NEW.event_type, NEW.payload->>'event_type', NEW.payload #>> '{payload,event_type}', 'NONE');
  
  NEW.shipping_revenue := NULLIF(COALESCE(NEW.shipping_revenue::text, NEW.payload->>'shipping_revenue', NEW.payload #>> '{payload,shipping_revenue}'), '')::NUMERIC;
  NEW.status := COALESCE(NEW.status, NEW.payload->>'status', NEW.payload #>> '{payload,status}');
  
  NEW.ingested_at_utc := COALESCE(NEW.ingested_at_utc, NEW.payload->>'ingested_at_utc', NEW.payload #>> '{payload,ingested_at_utc}');
  NEW.occurred_at_utc := COALESCE(NEW.occurred_at_utc, NEW.payload->>'occurred_at_utc', NEW.payload #>> '{payload,occurred_at_utc}');
  NEW.location_type := COALESCE(NEW.location_type, NEW.payload->>'location_type', NEW.payload #>> '{payload,location_type}');
  NEW.city_name := COALESCE(NEW.city_name, NEW.payload->>'city_name', NEW.payload #>> '{payload,city_name}');
  NEW.country := COALESCE(NEW.country, NEW.payload->>'country', NEW.payload #>> '{payload,country}');
  NEW.store_name := COALESCE(NEW.store_name, NEW.payload->>'store_name', NEW.payload #>> '{payload,store_name}');
  
  NEW.spend_date := NULLIF(COALESCE(NEW.spend_date::text, NEW.payload->>'spend_date', NEW.payload #>> '{payload,spend_date}'), '')::DATE;
  NEW.spend_amount := NULLIF(COALESCE(NEW.spend_amount::text, NEW.payload->>'spend_amount', NEW.payload #>> '{payload,spend_amount}'), '')::NUMERIC;
  NEW.reason := COALESCE(NEW.reason, NEW.payload->>'reason', NEW.payload #>> '{payload,reason}');
  
  -- 7. Timestamp Konversi
  NEW.valid_from_ts := NULLIF(COALESCE(NEW.valid_from_ts::text, NEW.valid_from_utc), '')::TIMESTAMPTZ;
  NEW.valid_to_ts := NULLIF(COALESCE(NEW.valid_to_ts::text, NEW.valid_to_utc), '')::TIMESTAMPTZ;
  NEW.ordered_to_ts := NULLIF(COALESCE(NEW.ordered_to_ts::text, NEW.payload->>'ordered_to_utc'), '')::TIMESTAMPTZ;
  NEW.occurred_ts := NULLIF(COALESCE(NEW.occurred_ts::text, NEW.occurred_at_utc), '')::TIMESTAMPTZ;
  NEW.source_ingested_ts := NULLIF(COALESCE(NEW.source_ingested_ts::text, NEW.ingested_at_utc), '')::TIMESTAMPTZ;
  
  -- 8. Sumber ID & Penentuan Target Tabel - FIX priority payment_id first
  NEW.source_record_id := COALESCE(
      NEW.payment_id,
      NEW.event_id, 
      NEW.promotion_id, 
      NEW.return_id, 
      NEW.payload->>'ticket_id', 
      NEW.refund_id, 
      NEW.session_id, 
      NEW.order_id, 
      NEW.address_id, 
      NEW.source_row_id, 
      NEW.customer_id, 
      NULLIF(CONCAT(COALESCE(NEW.product_id,''),'|',COALESCE(NEW.store_id,''),'|',COALESCE(NEW.snapshot_date,'')), '||')
  );

  NEW.target_table := CASE 
    WHEN LOWER(NEW.file_name) LIKE '%web_events%' THEN 'silver.web_events' 
    WHEN LOWER(NEW.file_name) LIKE '%customer_profiles%' THEN 'silver.customer_profiles' 
    WHEN LOWER(NEW.file_name) LIKE '%order_promotions%' THEN 'silver.order_promotions' 
    WHEN LOWER(NEW.file_name) LIKE '%product_categories%' THEN 'silver.product_categories' 
    WHEN LOWER(NEW.file_name) LIKE '%support_events%' THEN 'silver.support_events' 
    WHEN LOWER(NEW.file_name) LIKE '%return_events%' THEN 'silver.return_events' 
    WHEN LOWER(NEW.file_name) LIKE '%refund_events%' THEN 'silver.refund_events' 
    WHEN LOWER(NEW.file_name) LIKE '%payment_events%' THEN 'silver.payment_events' 
    WHEN LOWER(NEW.file_name) LIKE '%promotions%' THEN 'silver.promotions' 
    WHEN LOWER(NEW.file_name) LIKE '%order_items%' THEN 'silver.order_items' 
    WHEN LOWER(NEW.file_name) LIKE '%customer_addresses%' THEN 'silver.customer_addresses' 
    WHEN LOWER(NEW.file_name) LIKE '%campaign_spend%' THEN 'silver.campaign_spend' 
    WHEN LOWER(NEW.file_name) LIKE '%sales_channels%' THEN 'silver.sales_channels' 
    WHEN LOWER(NEW.file_name) LIKE '%city_reference%' THEN 'silver.city_reference' 
    WHEN LOWER(NEW.file_name) LIKE '%stores%' THEN 'silver.stores' 
    WHEN LOWER(NEW.file_name) LIKE '%customers%' THEN 'silver.customers' 
    WHEN LOWER(NEW.file_name) LIKE '%products%' THEN 'silver.products' 
    WHEN LOWER(NEW.file_name) LIKE '%orders%' THEN 'silver.orders' 
    WHEN LOWER(NEW.file_name) LIKE '%inventory%' THEN 'silver.inventory_snapshots' 
    ELSE 'silver.' || REPLACE(REPLACE(NEW.file_name, '.csv',''), '.json','') 
  END;

  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_fill_raw_data ON bronze.raw_data;
CREATE TRIGGER trg_fill_raw_data BEFORE INSERT OR UPDATE ON bronze.raw_data FOR EACH ROW EXECUTE FUNCTION bronze.fill_raw_data();

CREATE INDEX IF NOT EXISTS idx_raw_source_record_id ON bronze.raw_data(source_record_id); 
CREATE INDEX IF NOT EXISTS idx_raw_target_table ON bronze.raw_data(target_table); 
CREATE INDEX IF NOT EXISTS idx_raw_checksum ON bronze.raw_data(checksum); 
CREATE INDEX IF NOT EXISTS idx_raw_ingested_at ON bronze.raw_data(ingested_at); 
CREATE INDEX IF NOT EXISTS idx_ingested_file_name ON bronze.raw_data(file_name); 
CREATE INDEX IF NOT EXISTS idx_ingested_order_id ON bronze.raw_data(order_id); 
CREATE INDEX IF NOT EXISTS idx_ingested_customer_id ON bronze.raw_data(customer_id); 
CREATE INDEX IF NOT EXISTS idx_ingested_product_id ON bronze.raw_data(product_id); 
CREATE INDEX IF NOT EXISTS idx_ingested_store_id ON bronze.raw_data(store_id); 
CREATE INDEX IF NOT EXISTS idx_ingested_payload ON bronze.raw_data USING gin (payload); 
CREATE INDEX IF NOT EXISTS idx_raw_event_id ON bronze.raw_data(event_id); 
CREATE INDEX IF NOT EXISTS idx_raw_refund_id ON bronze.raw_data(refund_id); 
CREATE INDEX IF NOT EXISTS idx_raw_address_id ON bronze.raw_data(address_id);
CREATE INDEX IF NOT EXISTS idx_raw_payment_id ON bronze.raw_data(payment_id);
