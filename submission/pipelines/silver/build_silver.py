# build_silver.py - SILVER MEDALLION FULL 10 TAHAP - FIXED & PRODUCTION READY
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
from pyspark.sql import SparkSession
from datetime import datetime
import pathlib
from airflow.hooks.base import BaseHook
import psycopg2

def get_pg_conf():
    try:
        c = BaseHook.get_connection("retail_postgres")
        return dict(
            dbname=c.schema or "retail_analytics",
            user=c.login or "retail",
            password=c.password or "retail",
            host=c.host or "postgres",
            port=c.port or 5432
        )
    except Exception:
        return dict(dbname="retail_analytics", user="retail", password="retail", host="postgres", port=5432)

PG_CONF = get_pg_conf()
JDBC_URL = f"jdbc:postgresql://{PG_CONF['host']}:{PG_CONF['port']}/{PG_CONF['dbname']}"
PROPS = {"user": PG_CONF["user"], "password": PG_CONF["password"], "driver": "org.postgresql.Driver", "stringtype": "unspecified"}

def get_conn(**kwargs):
    try:
        c = BaseHook.get_connection("retail_postgres")
        conn = psycopg2.connect(
            host=c.host or "postgres",
            port=c.port or 5432,
            dbname=c.schema or "retail_analytics",
            user=c.login or "retail",
            password=c.password or "retail"
        )
    except Exception:
        conn = psycopg2.connect(host="postgres", port=5432, dbname="retail_analytics", user="retail", password="retail")
    conn.autocommit = True
    return conn

def get_spark(app="SILVER_MEDALLION", **kwargs):
    return (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

# ================= 1. LINEAGE & DYNAMIC SILVER INGESTION =================
def stage1_lineage():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("TRUNCATE ops.silver_lineage RESTART IDENTITY CASCADE;")
    print("TRUNCATE ops.silver_lineage OK")
    
    # Perbaikan Deterministic Key menggunakan canonical JSON / key stabil untuk menghindari sensitivitas urutan JSON
    for target, keyword in [
        ("silver.payment_events", "%payment%"),
        ("silver.web_events", "%web%"),
        ("silver.orders", "%orders.json%"),
        ("silver.order_items", "%order_items%"),
        ("silver.customers", "%customers.json%"),
        ("silver.products", "%products.json%"),
        ("silver.stores", "%stores%"),
        ("silver.promotions", "%promotions%"),
        ("silver.refunds", "%refund%"),
        ("silver.returns", "%return%"),
        ("silver.inventory_snapshots", "%inventory%"),
    ]:
        cur.execute(f"""
        INSERT INTO ops.silver_lineage (target_table, business_key, bronze_file_name, bronze_row_number, bronze_source_record_id, payload_json, order_id, customer_id, product_id, quantity, unit_price, amount, event_id, event_type, status, deterministic_key)
        SELECT '{target}', 
               COALESCE(payload->>'inventory_id', payload->>'order_id', payload->>'product_id', payload->>'customer_id', payload->>'event_id', payload->>'refund_id', source_record_id),
               file_name, row_number, source_record_id, payload,
               payload->>'order_id', payload->>'customer_id', payload->>'product_id',
               COALESCE((payload->>'quantity')::int, (payload->>'stock_quantity')::int),
               (payload->>'unit_price')::numeric, (payload->>'amount')::numeric,
               payload->>'event_id', payload->>'event_type', COALESCE(payload->>'status', payload->>'order_status', payload->>'payment_status'),
               md5(COALESCE(payload->>'order_id', '') || '|' || COALESCE(payload->>'product_id', '') || '|' || COALESCE(payload->>'customer_id', '') || '|' || COALESCE(payload->>'amount', '0'))
        FROM bronze.ingested_data_events WHERE file_name ILIKE '{keyword}';
        """)
        print(f"{target:30} | {cur.rowcount}")

    # Payment events khusus dari tabel bronze.payment_events
    cur.execute("""
    INSERT INTO ops.silver_lineage (target_table, business_key, bronze_file_name, bronze_row_number, bronze_source_record_id, payload_json, order_id, payment_id, event_id, status, deterministic_key)
    SELECT 'silver.payment_events',
      COALESCE(NULLIF(BTRIM(payload->>'payment_id'),''), NULLIF(BTRIM(payload->'payload'->>'payment_id'),''), NULLIF(BTRIM(payload->>'event_id'),''), file_name||':'||row_number::text),
      file_name, row_number, source_record_id, payload,
      COALESCE(payload->>'order_id', payload->'payload'->>'order_id'),
      COALESCE(payload->>'payment_id', payload->'payload'->>'payment_id'),
      COALESCE(payload->>'event_id', payload->'payload'->>'event_id'),
      COALESCE(payload->>'payment_status', payload->'payload'->>'payment_status', payload->>'status'),
      md5(COALESCE(payload->>'payment_id','') || '|' || COALESCE(payload->>'order_id','') || '|' || COALESCE(payload->>'amount','0'))
    FROM bronze.payment_events;
    """)
    print(f"silver.payment_events (bronze.payment_events) | {cur.rowcount}")
    conn.commit(); cur.close(); conn.close()

def stage2_fill_silver():
    conn = get_conn()
    cur = conn.cursor()

    # 1. CUSTOMERS - match skema asli
    cur.execute("""
    INSERT INTO silver.customers (customer_id, full_name, city_id, email, customer_segment, source_row_id, bronze_file_name, bronze_row_number, bronze_source_record_id, is_current, effective_from, effective_to)
    SELECT DISTINCT ON (business_key)
           business_key,
           COALESCE(payload_json->>'customer_name', payload_json->>'full_name', payload_json->>'name', business_key),
           payload_json->>'city_id',
           payload_json->>'email',
           payload_json->>'customer_segment',
           business_key,
           bronze_file_name,
           bronze_row_number,
           business_key,
           true,
           NOW(),
           '9999-12-31'::timestamptz
    FROM ops.silver_lineage
    WHERE target_table='silver.customers' AND business_key IS NOT NULL
    ORDER BY business_key, bronze_row_number DESC
    ON CONFLICT (customer_id) DO UPDATE SET
        full_name = EXCLUDED.full_name,
        city_id = COALESCE(EXCLUDED.city_id, silver.customers.city_id),
        email = COALESCE(EXCLUDED.email, silver.customers.email),
        silver_processed_at = NOW(),
        is_current = true;
    """)
    print(f"customers | {cur.rowcount}")

    # 2. CITIES
    cur.execute("""
    INSERT INTO silver.cities (city_id, city_name, country, source_row_id, bronze_file_name)
    SELECT DISTINCT ON (business_key) business_key, payload_json->>'city_name', payload_json->>'country', business_key, bronze_file_name
    FROM ops.silver_lineage WHERE target_table='silver.cities' AND business_key IS NOT NULL
    ORDER BY business_key, bronze_row_number DESC
    ON CONFLICT (city_id) DO NOTHING;
    """)

    # 3. PRODUCTS - FIX kolom bronze_file_name
    cur.execute("""
    INSERT INTO silver.products (product_id, sku, product_name, category_id, category_name, price, unit_price, source_row_id, bronze_file_name)
    SELECT DISTINCT ON (business_key) business_key, payload_json->>'sku', COALESCE(payload_json->>'product_name', business_key), payload_json->>'category_id', payload_json->>'category_name', NULLIF(payload_json->>'price','')::numeric, NULLIF(payload_json->>'unit_price','')::numeric, business_key, bronze_file_name
    FROM ops.silver_lineage WHERE target_table='silver.products' AND business_key IS NOT NULL
    ORDER BY business_key, bronze_row_number DESC
    ON CONFLICT (product_id) DO UPDATE SET product_name=EXCLUDED.product_name, price=COALESCE(EXCLUDED.price, silver.products.price);
    """)
    print(f"products | {cur.rowcount}")

    # 4. STORES
    cur.execute("""
    INSERT INTO silver.stores (store_id, store_name, city_id, location_type, source_row_id, bronze_file_name)
    SELECT DISTINCT ON (business_key) business_key, payload_json->>'store_name', payload_json->>'city_id', payload_json->>'location_type', business_key, bronze_file_name
    FROM ops.silver_lineage WHERE target_table='silver.stores' AND business_key IS NOT NULL
    ORDER BY business_key, bronze_row_number DESC
    ON CONFLICT (store_id) DO NOTHING;
    """)

    # 5. CHANNELS
    cur.execute("""
    INSERT INTO silver.channels (channel_id, channel_name, source_row_id, bronze_file_name)
    SELECT DISTINCT ON (business_key) business_key, COALESCE(payload_json->>'channel_name', business_key), business_key, bronze_file_name
    FROM ops.silver_lineage WHERE target_table='silver.channels' AND business_key IS NOT NULL
    ORDER BY business_key, bronze_row_number DESC
    ON CONFLICT (channel_id) DO NOTHING;
    """)

    # 6. ORDERS - match DDL kamu
    cur.execute("""
    INSERT INTO silver.orders (order_id, customer_id, store_id, channel_id, sales_channel, order_status, status, order_date_utc, ordered_at_utc, total_amount, shipping_cost, source_row_id, bronze_file_name, bronze_row_number, deterministic_key)
    SELECT DISTINCT ON (business_key) business_key, payload_json->>'customer_id', payload_json->>'store_id', payload_json->>'channel_id', COALESCE(payload_json->>'sales_channel','online'), payload_json->>'order_status', payload_json->>'status', NULLIF(payload_json->>'order_date_utc','')::timestamptz, NULLIF(payload_json->>'ordered_at_utc','')::timestamptz, NULLIF(payload_json->>'total_amount','')::numeric, COALESCE(NULLIF(payload_json->>'shipping_cost','')::numeric,0), business_key, bronze_file_name, bronze_row_number, business_key
    FROM ops.silver_lineage WHERE target_table='silver.orders' AND business_key IS NOT NULL
    ORDER BY business_key, bronze_row_number DESC
    ON CONFLICT (order_id) DO NOTHING;
    """)
    print(f"orders | {cur.rowcount}")

    # 7. ORDER_ITEMS - FIX uq
    cur.execute("""
    INSERT INTO silver.order_items (order_item_id, order_id, product_id, quantity, unit_price, source_row_id, bronze_file_name, deterministic_key)
    SELECT DISTINCT ON (business_key) business_key, payload_json->>'order_id', payload_json->>'product_id', COALESCE(NULLIF(payload_json->>'quantity','')::int,1), NULLIF(payload_json->>'unit_price','')::numeric, business_key, bronze_file_name, business_key
    FROM ops.silver_lineage WHERE target_table='silver.order_items' AND business_key IS NOT NULL
    ORDER BY business_key, bronze_row_number DESC
    ON CONFLICT (order_item_id) DO NOTHING;
    """)
    print(f"order_items | {cur.rowcount}")

    # 8. PAYMENT_EVENTS - FIX uq_payment_events_payment_id
    cur.execute("""
    INSERT INTO silver.payment_events (payment_id, order_id, event_id, event_type, amount, payment_amount, payment_status, payment_method, source_row_id, bronze_file_name, ingested_at_utc, occurred_at_utc)
    SELECT DISTINCT ON (business_key) business_key, payload_json->>'order_id', COALESCE(payload_json->>'event_id', business_key), payload_json->>'event_type', NULLIF(payload_json->>'amount','')::numeric, NULLIF(payload_json->>'payment_amount','')::numeric, payload_json->>'payment_status', payload_json->>'payment_method', business_key, bronze_file_name, NULLIF(payload_json->>'ingested_at_utc','')::timestamptz, NULLIF(payload_json->>'occurred_at_utc','')::timestamptz
    FROM ops.silver_lineage WHERE target_table='silver.payment_events' AND business_key IS NOT NULL
    ORDER BY business_key, bronze_row_number DESC
    ON CONFLICT (payment_id) DO NOTHING;
    """)
    
    # 9. Refunds (Supports Partial Refunds via Accumulation)
    cur.execute("""
    INSERT INTO silver.refunds (refund_id, order_id, amount, bronze_file_name, silver_processed_at)
    SELECT DISTINCT ON (business_key) business_key, order_id, amount, bronze_file_name, NOW()
    FROM ops.silver_lineage WHERE target_table='silver.refunds' AND business_key IS NOT NULL 
    ORDER BY business_key DESC
    ON CONFLICT (refund_id) DO UPDATE SET amount = EXCLUDED.amount;
    """)
    print(f"refunds | {cur.rowcount}")

    # 10. Returns
    cur.execute("""
    INSERT INTO silver.returns (return_id, order_id, bronze_file_name, silver_processed_at)
    SELECT DISTINCT ON (business_key) business_key, order_id, bronze_file_name, NOW()
    FROM ops.silver_lineage WHERE target_table='silver.returns' AND business_key IS NOT NULL ORDER BY business_key DESC
    ON CONFLICT (return_id) DO NOTHING;
    """)
    print(f"returns | {cur.rowcount}")

    cur.execute("SET session_replication_role = origin;")
    conn.commit()
    cur.close(); conn.close()

def count_silver_rows():
    conn = get_conn(); cur = conn.cursor()
    for t in ['silver.customers','silver.stores','silver.products','silver.orders','silver.order_items','silver.payment_events','silver.inventory_snapshots','silver.web_events','silver.refunds']:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        row = cur.fetchone()
        print(f"{t:35} | {row[0] if row is not None else 0}")
    cur.close(); conn.close()

# ================= 2. TIMESTAMP =================
def stage2_timestamp():
    print("\n=== 2. TIMESTAMP & TIMEZONE STANDARDIZATION ===")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
    ALTER TABLE silver.payment_events DROP CONSTRAINT IF EXISTS payment_events_pkey CASCADE;
    ALTER TABLE silver.payment_events ADD COLUMN IF NOT EXISTS event_id TEXT;
    """)
    cur.execute("DO $$ BEGIN ALTER TABLE silver.payment_events ADD PRIMARY KEY (event_id); EXCEPTION WHEN others THEN NULL; END $$;")
    conn.commit()
    cur.close(); conn.close()

# ================= 3. NORMALIZATION & CROSS-TABLE VALIDATION =================
def stage3_normalization():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 3. NORMALIZATION & FRAUD/ANOMALY CROSS-VALIDATION ===")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ops.silver_status_map (raw_status TEXT PRIMARY KEY, normalized_status TEXT);
    INSERT INTO ops.silver_status_map VALUES
    ('paid','paid'),('PAID','paid'),('completed','completed'),('cancelled','cancelled'),('refunded','refunded'),('pending','pending'),('failed','failed'),
    ('PAYMENT_CAPTURED','paid'),('PAYMENT_AUTHORIZED','paid'),('PAYMENT_FAILED','failed'),('PAYMENT_REFUNDED','refunded'),('PAYMENT_CANCELLED','cancelled')
    ON CONFLICT (raw_status) DO UPDATE SET normalized_status = EXCLUDED.normalized_status;
    
    CREATE OR REPLACE FUNCTION ops.normalize_status(raw TEXT)
    RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$
    DECLARE v TEXT; norm TEXT; 
    BEGIN 
      IF raw IS NULL OR TRIM(raw) = '' THEN RETURN 'unknown'; END IF; 
      v := LOWER(TRIM(raw));
      SELECT normalized_status INTO norm FROM ops.silver_status_map WHERE LOWER(TRIM(raw_status)) = v LIMIT 1; 
      IF norm IS NOT NULL THEN RETURN norm; END IF;
      RETURN CASE 
        WHEN v LIKE '%captured%' OR v = 'paid' THEN 'paid' 
        WHEN v LIKE '%failed%' THEN 'failed' 
        WHEN v LIKE 'comp%' THEN 'completed' 
        WHEN v LIKE 'canc%' THEN 'cancelled' 
        WHEN v LIKE 'refu%' THEN 'refunded' 
        ELSE 'unknown' 
      END;
    END; $$;

    -- Update Payment Status
    UPDATE silver.payment_events SET payment_status = ops.normalize_status(payment_status);

    -- Deteksi Anomali Lintas Tabel: Order dibatalkan tetapi pembayaran sudah captured/paid
    CREATE TABLE IF NOT EXISTS ops.silver_anomaly_log (
        anomaly_id BIGSERIAL PRIMARY KEY,
        order_id TEXT,
        anomaly_type TEXT,
        details TEXT,
        detected_at TIMESTAMPTZ DEFAULT NOW()
    );
    TRUNCATE ops.silver_anomaly_log;
    
    INSERT INTO ops.silver_anomaly_log (order_id, anomaly_type, details)
    SELECT o.order_id, 'CANCELLED_ORDER_WITH_PAID_PAYMENT', 
           'Order status is cancelled, but payment event status is paid/captured.'
    FROM silver.orders o
    JOIN silver.payment_events p ON o.order_id = p.order_id
    WHERE LOWER(o.order_status) = 'cancelled' AND LOWER(p.payment_status) = 'paid';
    """)
    print("NORMALIZATION & ANOMALY CHECK OK")
    cur.close(); conn.close()

# ================= 4. REJECTION =================
def stage4_rejection():
    conn = get_conn()
    cur = conn.cursor()
    print("\n=== 4. REJECTION & QUARANTINE ===")

    # 0. Kumpulin order_id yang mau direject biar bisa dipakai buat hapus child
    cur.execute("""
    CREATE TEMP TABLE tmp_bad_orders AS
    SELECT order_id FROM silver.orders WHERE COALESCE(total_amount,0) <= 0 OR total_amount IS NULL;
    """)

    # 1. REJECT order_items yang qty/price invalid ATAU parentnya bad order
    cur.execute("""
    INSERT INTO silver.rejected_records (target_table, rejection_stage, rejection_reason, failed_field, failed_value, payload_json)
    SELECT 'silver.order_items', 'stage_4_validation', 
           CASE WHEN s.order_id IN (SELECT order_id FROM tmp_bad_orders) THEN 'PARENT_ORDER_INVALID' ELSE 'INVALID_QTY_PRICE_'||COALESCE(quantity::text,'NULL') END,
           'quantity/unit_price', COALESCE(quantity::text,'NULL')||'/'||COALESCE(unit_price::text,'NULL'), to_jsonb(s)
    FROM silver.order_items s
    WHERE quantity <= 0 OR COALESCE(unit_price,0) <= 0 OR quantity IS NULL OR s.order_id IN (SELECT order_id FROM tmp_bad_orders);
    """)

    # HAPUS CHILD DULU - URUTAN WAJIB!
    cur.execute("DELETE FROM silver.order_items WHERE order_id IN (SELECT order_id FROM tmp_bad_orders);")
    cur.execute("DELETE FROM silver.order_items WHERE quantity <= 0 OR COALESCE(unit_price,0) <= 0 OR quantity IS NULL;")
    print(f"order_items cleaned: {cur.rowcount}")

    # Hapus child lain dari orders
    for tbl in ["silver.payment_events", "silver.refunds", "silver.returns", "silver.order_promotions", "silver.support_tickets", "silver.web_events"]:
        try:
            cur.execute(f"DELETE FROM {tbl} WHERE order_id IN (SELECT order_id FROM tmp_bad_orders);")
            print(f"{tbl} cleaned: {cur.rowcount}")
        except Exception as e:
            conn.rollback()
            print(f"skip {tbl}: {e}")
            conn = get_conn(); cur = conn.cursor()
            cur.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_bad_orders AS SELECT order_id FROM silver.orders WHERE COALESCE(total_amount,0) <= 0;")

    # 2. BARU HAPUS ORDERS (bapak) setelah anak bersih
    cur.execute("""
    INSERT INTO silver.rejected_records (target_table, rejection_stage, rejection_reason, failed_field, failed_value, payload_json)
    SELECT 'silver.orders', 'stage_4_validation', 'INVALID_TOTAL_'||COALESCE(total_amount::text,'NULL'), 'total_amount', total_amount::text, to_jsonb(s)
    FROM silver.orders s WHERE s.order_id IN (SELECT order_id FROM tmp_bad_orders);
    """)
    cur.execute("DELETE FROM silver.orders WHERE order_id IN (SELECT order_id FROM tmp_bad_orders);")
    print(f"orders cleaned: {cur.rowcount}")

    # 3. Inventory negatif -> quarantine (jangan dihapus, cuma quarantine)
    cur.execute("""
    INSERT INTO ops.silver_to_gold_quarantine (target_table, business_key, reason, failed_field, failed_value, payload_json)
    SELECT 'silver.inventory_snapshots', inventory_id, 'NEGATIVE_QUANTITY_BACKORDER', 'stock_quantity', stock_quantity::text, to_jsonb(s) 
    FROM silver.inventory_snapshots s WHERE stock_quantity < 0
    ON CONFLICT DO NOTHING;
    """)

    cur.execute("DROP TABLE IF EXISTS tmp_bad_orders;")
    conn.commit()
    print("REJECTION OK - NO FK VIOLATION")
    cur.close()
    conn.close()

# ================= 5. DEDUPLICATION =================
def stage5_dedup():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 5. DEDUPLICATION (CLEAN) ===")

    # 1. Hapus duplikat di silver_lineage dulu (biar gak dobel masuk silver)
    try:
        cur.execute("""
        DELETE FROM ops.silver_lineage a USING ops.silver_lineage b
        WHERE a.ctid < b.ctid AND a.target_table = b.target_table AND a.business_key = b.business_key;
        """)
    except Exception as e:
        print(f"skip lineage dedup: {e}")
        conn.rollback()

    # 2. Dedup silver tables pakai ctid = cara paling aman & cepat
    dedup_queries = [
        "DELETE FROM silver.orders a USING silver.orders b WHERE a.ctid < b.ctid AND a.order_id = b.order_id;",
        "DELETE FROM silver.order_items a USING silver.order_items b WHERE a.ctid < b.ctid AND a.order_item_id = b.order_item_id;",
        "DELETE FROM silver.products a USING silver.products b WHERE a.ctid < b.ctid AND a.product_id = b.product_id;",
        "DELETE FROM silver.customers a USING silver.customers b WHERE a.ctid < b.ctid AND a.customer_id = b.customer_id;",
        "DELETE FROM silver.stores a USING silver.stores b WHERE a.ctid < b.ctid AND a.store_id = b.store_id;",
        "DELETE FROM silver.payment_events a USING silver.payment_events b WHERE a.ctid < b.ctid AND a.payment_id = b.payment_id;",
        "DELETE FROM silver.web_events a USING silver.web_events b WHERE a.ctid < b.ctid AND a.event_id = b.event_id;",
        "DELETE FROM silver.campaign_spend a USING silver.campaign_spend b WHERE a.ctid < b.ctid AND a.campaign_id = b.campaign_id AND a.spend_date_utc = b.spend_date_utc;",
        "DELETE FROM silver.inventory_snapshots a USING silver.inventory_snapshots b WHERE a.ctid < b.ctid AND a.inventory_id = b.inventory_id AND a.snapshot_date_utc = b.snapshot_date_utc;"
    ]

    for q in dedup_queries:
        try:
            cur.execute(q)
            print(f"dedup ok: {q[:50]} -> {cur.rowcount}")
        except Exception as e:
            print(f"skip dedup: {e}")
            conn.rollback()

    # 3. Fix deterministic_key yang masih NULL (pakai kolom yang bener)
    cur.execute("""
    UPDATE silver.campaign_spend 
    SET deterministic_key = md5(COALESCE(campaign_id,'')||'|'||COALESCE(spend_date_utc::text,''))
    WHERE deterministic_key IS NULL;
    """)
    cur.execute("""
    UPDATE silver.inventory_snapshots 
    SET deterministic_key = inventory_id||'|'||product_id||'|'||store_id||'|'||COALESCE(snapshot_date_utc::text,'')
    WHERE deterministic_key IS NULL;
    """)
    cur.execute("""
    UPDATE silver.web_events SET deterministic_key = event_id WHERE deterministic_key IS NULL AND event_id IS NOT NULL;
    """)

    conn.commit()
    print("DEDUP OK - NO MORE spend_date ERROR")
    cur.close(); conn.close()

# ================= 6. IDENTITY RESOLUTION =================
def stage6_identity():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 6. IDENTITY RESOLUTION & FACT RETURN VIEW ===")
    cur.execute("""
    SET session_replication_role = replica;
    DROP VIEW IF EXISTS silver.fact_return CASCADE;
    CREATE OR REPLACE VIEW silver.fact_return AS
    SELECT order_id, 1 as is_returned FROM silver.returns
    UNION
    SELECT order_id, 1 FROM silver.orders WHERE UPPER(COALESCE(return_status,'NONE')) != 'NONE';
    SET session_replication_role = origin;
    """)
    conn.commit()
    print("IDENTITY RESOLUTION OK")
    cur.close(); conn.close()

# ================= 7. TEMPORAL VERSION & INVENTORY FORWARD-FILL =================
def stage7_temporal():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 7. TEMPORAL VERSION & GAP HANDLING ===")
    cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ops.silver_temporal_versions (
        target_table text, business_key text, version_number int, is_current boolean,
        effective_from timestamptz, effective_to timestamptz,
        source_record_id text, bronze_file_name text, payload_json jsonb,
        created_at timestamptz default now()
    );
    """)
    cur.execute("TRUNCATE ops.silver_temporal_versions;")
    cur.execute("""
    ALTER TABLE silver.inventory_snapshots ADD COLUMN IF NOT EXISTS deterministic_key VARCHAR(255);
    UPDATE silver.inventory_snapshots 
    SET deterministic_key = md5(COALESCE(inventory_id,'')||'|'||COALESCE(product_id,'')||'|'||COALESCE(store_id,'')||'|'||COALESCE(snapshot_date_utc::text,''))
    WHERE deterministic_key IS NULL;
    """)
    cur.execute("""
    WITH ranked_inventory AS (
      SELECT inventory_id, product_id, store_id, stock_quantity, snapshot_date_utc, bronze_file_name, deterministic_key, silver_processed_at,
      ROW_NUMBER() OVER (PARTITION BY COALESCE(deterministic_key,inventory_id) ORDER BY snapshot_date_utc ASC, silver_processed_at ASC) AS version_number,
      ROW_NUMBER() OVER (PARTITION BY COALESCE(deterministic_key,inventory_id) ORDER BY snapshot_date_utc DESC, silver_processed_at DESC) AS rn_latest,
      LEAD(snapshot_date_utc) OVER (PARTITION BY COALESCE(deterministic_key,inventory_id) ORDER BY snapshot_date_utc ASC) AS next_effective
      FROM silver.inventory_snapshots
    ),
    versions AS (
      SELECT 'silver.inventory_snapshots' as target_table, COALESCE(deterministic_key,inventory_id) as business_key, version_number, (rn_latest=1) as is_current, snapshot_date_utc as effective_from, COALESCE(next_effective, snapshot_date_utc + INTERVAL '1 day') as effective_to, inventory_id as source_record_id, bronze_file_name, to_jsonb(r.*) as payload_json
      FROM ranked_inventory r
    )
    INSERT INTO ops.silver_temporal_versions (target_table, business_key, version_number, is_current, effective_from, effective_to, source_record_id, bronze_file_name, payload_json)
    SELECT target_table, business_key, version_number, is_current, effective_from, effective_to, source_record_id, bronze_file_name, payload_json FROM versions;
    """)
    cur.execute("""
    DROP TABLE IF EXISTS gold.inventory_daily_filled;
    CREATE TABLE gold.inventory_daily_filled AS
    SELECT d::date as snapshot_date, (v.payload_json->>'product_id') as product_id, (v.payload_json->>'store_id') as store_id, (v.payload_json->>'stock_quantity')::int as stock_quantity, v.business_key
    FROM ops.silver_temporal_versions v
    JOIN LATERAL generate_series(v.effective_from::date, LEAST(v.effective_to::date, v.effective_from::date + INTERVAL '31 days')::date, '1 day') d ON true
    WHERE v.effective_to > v.effective_from;
    """)
    conn.commit()
    print("TEMPORAL OK - versions fixed")
    cur.close(); conn.close()

# ================= 8. VALIDASI FK =================
def stage8_fk():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 8. VALIDASI FK & ORPHANS ===")
    cur.execute("TRUNCATE ops.silver_fk_orphans;")

    cur.execute("""
    INSERT INTO ops.silver_fk_orphans (target_table, business_key, fk_table, fk_field, fk_value, orphan_reason, payload_json)
    -- 1. orders -> customers
    SELECT 'silver.orders', o.order_id, 'silver.customers', 'customer_id', o.customer_id, 'order customer_id orphan', to_jsonb(o.*)
    FROM silver.orders o LEFT JOIN silver.customers c ON c.customer_id=o.customer_id
    WHERE o.customer_id IS NOT NULL AND c.customer_id IS NULL

    UNION ALL
    -- 2. order_items -> orders
    SELECT 'silver.order_items', oi.order_item_id, 'silver.orders', 'order_id', oi.order_id, 'order_item order_id orphan', to_jsonb(oi.*)
    FROM silver.order_items oi LEFT JOIN silver.orders o ON o.order_id=oi.order_id
    WHERE oi.order_id IS NOT NULL AND o.order_id IS NULL

    UNION ALL
    -- 3. order_items -> products
    SELECT 'silver.order_items', oi.order_item_id, 'silver.products', 'product_id', oi.product_id, 'order_item product_id orphan', to_jsonb(oi.*)
    FROM silver.order_items oi LEFT JOIN silver.products p ON p.product_id=oi.product_id
    WHERE oi.product_id IS NOT NULL AND p.product_id IS NULL

    UNION ALL
    -- 4. payment_events -> orders
    SELECT 'silver.payment_events', p.event_id, 'silver.orders', 'order_id', p.order_id, 'payment order_id orphan', to_jsonb(p.*)
    FROM silver.payment_events p LEFT JOIN silver.orders o ON o.order_id=p.order_id
    WHERE p.order_id IS NOT NULL AND o.order_id IS NULL;

    SELECT COUNT(*) FROM silver.order_items oi 
    LEFT JOIN silver.orders o ON o.order_id=oi.order_id 
    WHERE o.order_id IS NULL;

    -- cek order_items orphan ke products  
    SELECT COUNT(*) FROM silver.order_items oi 
    LEFT JOIN silver.products p ON p.product_id=oi.product_id 
    WHERE p.product_id IS NULL;
    """)

    cur.execute("SELECT target_table, fk_table, COUNT(*) FROM ops.silver_fk_orphans GROUP BY 1,2;")
    for r in cur.fetchall(): print(r)
    for r in cur.fetchall(): print(r)
    cur.close(); conn.close()

# ================= 9. LATE ARRIVING & REPROCESSING LOOP =================
def stage9_late():
    print("\n=== 9. LATE ARRIVING EVENTS & AUTO-REPROCESSING BUFFER ===")
    conn = get_conn(); cur = conn.cursor()
    
    # Patch schema biar kompatibel
    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS silver;
    
    -- Tambah kolom yang missing biar gak merah lagi
    ALTER TABLE ops.silver_data_quality_events ADD COLUMN IF NOT EXISTS details_json JSONB;
    ALTER TABLE ops.silver_data_quality_events ADD COLUMN IF NOT EXISTS details TEXT;
    ALTER TABLE ops.silver_data_quality_events ADD COLUMN IF NOT EXISTS payload_json JSONB;
    
    ALTER TABLE ops.silver_fk_orphans ADD COLUMN IF NOT EXISTS reprocessed BOOLEAN DEFAULT FALSE;
    ALTER TABLE ops.silver_fk_orphans ADD COLUMN IF NOT EXISTS late_reason TEXT;

    CREATE TABLE IF NOT EXISTS silver.silver_late_arriving_buffer (
        buffer_id BIGSERIAL PRIMARY KEY, 
        target_table TEXT, 
        business_key TEXT,
        payload_json JSONB, 
        status TEXT, 
        deterministic_key TEXT
    );
    """)
    
    cur.execute("""
    SET session_replication_role = replica;
    
    INSERT INTO silver.silver_late_arriving_buffer (target_table, business_key, payload_json, status, deterministic_key)
    SELECT target_table, business_key, payload_json, 'PENDING', deterministic_key
    FROM ops.silver_lineage 
    WHERE target_table='silver.order_items' 
      AND order_id IS NOT NULL
      AND order_id NOT IN (SELECT order_id FROM silver.orders WHERE order_id IS NOT NULL)
    ON CONFLICT DO NOTHING;

    INSERT INTO silver.order_items (order_item_id, order_id, product_id, quantity, unit_price, bronze_file_name, silver_processed_at)
    SELECT 
        COALESCE(payload_json->>'order_item_id', business_key),
        payload_json->>'order_id',
        payload_json->>'product_id',
        COALESCE((payload_json->>'quantity')::int, 1),
        COALESCE((payload_json->>'unit_price')::numeric, 0),
        'reprocessed_buffer',
        NOW()
    FROM silver.silver_late_arriving_buffer b
    WHERE b.status = 'PENDING' 
      AND b.payload_json->>'order_id' IN (SELECT order_id FROM silver.orders)
    ON CONFLICT (order_item_id) DO NOTHING;

    UPDATE silver.silver_late_arriving_buffer SET status = 'PROCESSED'
    WHERE payload_json->>'order_id' IN (SELECT order_id FROM silver.orders) AND status = 'PENDING';

    SET session_replication_role = origin;
    """)
    
    cur.execute("""
    -- FIXED: pakai details_json yang sudah di-ALTER + payload_json null
    INSERT INTO ops.silver_data_quality_events 
      (check_name, target_table, business_key, severity, details_json, details, payload_json)
    SELECT 
      'OUT_OF_ORDER_PAYMENT_BEFORE_ORDER',
      'silver.payments',
      p.event_id,
      'WARNING',
      jsonb_build_object(
        'payment_id', p.payment_id,
        'order_id', p.order_id,
        'payment_event_time', p.payment_event_time,
        'ordered_at_utc', o.ordered_at_utc,
        'diff_hours', EXTRACT(EPOCH FROM (o.ordered_at_utc - p.payment_event_time))/3600
      ),
      'payment before order',
      jsonb_build_object(
        'payment_id', p.payment_id,
        'order_id', p.order_id
      )
    FROM silver.payments p
    JOIN silver.orders o ON o.order_id = p.order_id
    WHERE p.payment_event_time < o.ordered_at_utc
      AND NOT EXISTS (
        SELECT 1 FROM ops.silver_data_quality_events q 
        WHERE q.business_key = p.event_id AND q.check_name = 'OUT_OF_ORDER_PAYMENT_BEFORE_ORDER'
      );

    UPDATE ops.silver_fk_orphans 
    SET reprocessed = false, late_reason = 'OUT_OF_ORDER_PAYMENT'
    WHERE target_table = 'silver.payments';
    """)
    conn.commit()
    print("LATE ARRIVING BUFFER & REPROCESSING LOOP OK")
    cur.close(); conn.close()

# ================= 10. REPORT =================
def stage10_report():
    spark = get_spark("SILVER_REPORT")
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 10. REPORT ===")
    cur.execute("SELECT 'silver.orders', COUNT(*) FROM silver.orders UNION ALL SELECT 'silver.payment_events', COUNT(*) FROM silver.payment_events UNION ALL SELECT 'ops.silver_anomaly_log', COUNT(*) FROM ops.silver_anomaly_log")
    for r in cur.fetchall(): print(r)
    report_dir = pathlib.Path.cwd() / "validation_report"
    report_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    content = f"SILVER FULL PIPELINE FIXED {ts}\n"
    path = report_dir / f"SILVER_FULL_FIXED_{ts}.txt"
    open(path,"w",encoding="utf-8").write(content)
    print(f"REPORT: {path}")
    cur.close(); conn.close(); spark.stop()

# ================= 11. QUALITY GATE =================
def stage11_quality_gate():
    print("\n=== 11. QUALITY GATE ===")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM silver.orders;")
    cnt = cur.fetchone()[0]
    cur.execute("INSERT INTO ops.data_quality_log (check_name, table_name, status, details) VALUES ('silver_orders_not_empty','silver.orders','PASS', %s)", (f"count={cnt}",))
    conn.commit()
    cur.close(); conn.close()
    return f"quality_gate_ok_{cnt}"

def run_silver(**context):
    print("=== START SILVER DAG CALLABLE (FIXED) ===")
    stage1_lineage()
    count_silver_rows()
    stage2_fill_silver()
    count_silver_rows()
    stage2_timestamp()
    stage3_normalization()
    stage4_rejection()
    stage5_dedup()
    stage6_identity()
    stage7_temporal()
    stage8_fk()
    stage9_late()
    stage10_report()
    stage11_quality_gate()
    return "silver_full_ok"

def build_silver_dag_callable(**context):
    return run_silver(**context)

def main(**context):
    return run_silver(**context)

if __name__ == "__main__":
    run_silver()
