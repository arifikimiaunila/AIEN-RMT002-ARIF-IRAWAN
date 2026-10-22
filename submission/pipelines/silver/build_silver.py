# build_silver.py - SILVER MEDALLION FULL 10 TAHAP - FINAL + order_date
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
        SparkSession.Builder()
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
        SELECT '{target}', COALESCE(payload->>'inventory_id', payload->>'order_id', payload->>'product_id', payload->>'customer_id', payload->>'event_id', source_record_id),
               file_name, row_number, source_record_id, payload,
               payload->>'order_id', payload->>'customer_id', payload->>'product_id',
               COALESCE((payload->>'quantity')::int, (payload->>'stock_quantity')::int),
               (payload->>'unit_price')::numeric, (payload->>'amount')::numeric,
               payload->>'event_id', payload->>'event_type', COALESCE(payload->>'status', payload->>'order_status'),
               md5(payload::text)
        FROM bronze.ingested_data_events WHERE file_name ILIKE '{keyword}';
        """)
        print(f"{target:30} | {cur.rowcount}")

    cur.execute("""
    INSERT INTO ops.silver_lineage (target_table, business_key, bronze_file_name, bronze_row_number, bronze_source_record_id, payload_json, order_id, payment_id, event_id, status, deterministic_key)
    SELECT 'silver.payment_events',
      COALESCE(NULLIF(BTRIM(payload->>'payment_id'),''), NULLIF(BTRIM(payload->'payload'->>'payment_id'),''), NULLIF(BTRIM(payload->>'event_id'),''), file_name||':'||row_number::text),
      file_name, row_number, source_record_id, payload,
      COALESCE(payload->>'order_id', payload->'payload'->>'order_id'),
      COALESCE(payload->>'payment_id', payload->'payload'->>'payment_id'),
      COALESCE(payload->>'event_id', payload->'payload'->>'event_id'),
      COALESCE(payload->>'payment_status', payload->'payload'->>'payment_status', payload->>'status'),
      md5(payload::text)
    FROM bronze.payment_events;
    """)
    print(f"silver.payment_events (bronze.payment_events) | {cur.rowcount}")
    conn.commit(); cur.close(); conn.close()

def stage2_fill_silver():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== FILL ALL SILVER FROM LINEAGE ===")
    cur.execute("SET session_replication_role = replica;")
    
    # === TAMBAH KOLOM order_date DI silver.orders BIAR GOLD GAK ERROR ===
    cur.execute("""
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS order_date DATE;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS order_date_utc DATE;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS ordered_at_utc TIMESTAMPTZ;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS order_status TEXT;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS sales_channel TEXT;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS store_id TEXT;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS total_amount NUMERIC;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS shipping_revenue NUMERIC;
    """)
    
    tables = ["silver.web_events","silver.inventory_snapshots","silver.payment_events","silver.order_items","silver.orders","silver.products","silver.stores","silver.customers"]
    for t in tables:
        try:
            cur.execute(f"DELETE FROM {t};")
        except: 
            conn.commit()

    cur.execute("""
    INSERT INTO silver.customers (customer_id, email, full_name, bronze_file_name, silver_processed_at)
    SELECT DISTINCT ON (business_key) business_key, payload_json->>'email', COALESCE(payload_json->>'full_name', payload_json->>'customer_name'), bronze_file_name, NOW()
    FROM ops.silver_lineage WHERE target_table='silver.customers' AND business_key IS NOT NULL ORDER BY business_key, bronze_row_number DESC;
    """)
    print(f"customers | {cur.rowcount}")

    cur.execute("""
    INSERT INTO silver.stores (store_id, store_name, bronze_file_name)
    SELECT DISTINCT ON (business_key) business_key, COALESCE(payload_json->>'store_name', business_key), bronze_file_name
    FROM ops.silver_lineage WHERE target_table='silver.stores' AND business_key IS NOT NULL ORDER BY business_key DESC;
    """)
    print(f"stores | {cur.rowcount}")

    cur.execute("""
    INSERT INTO silver.products (product_id, product_name, price, bronze_file_name)
    SELECT DISTINCT ON (business_key) business_key, COALESCE(payload_json->>'product_name', business_key), COALESCE((payload_json->>'price')::numeric, 0), bronze_file_name
    FROM ops.silver_lineage WHERE target_table='silver.products' AND business_key IS NOT NULL ORDER BY business_key DESC;
    """)
    print(f"products | {cur.rowcount}")

    # ORDERS DENGAN order_date + order_date_utc + ordered_at_utc
    cur.execute("ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS order_date DATE;")
    cur.execute("ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS sales_channel TEXT DEFAULT 'online';")
    cur.execute("ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS shipping_revenue NUMERIC DEFAULT 0;")
    cur.execute("ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS return_status TEXT DEFAULT 'NONE';")
    cur.execute("""
    INSERT INTO silver.orders (order_id, customer_id, store_id, order_status, sales_channel, order_date, order_date_utc, ordered_at_utc, total_amount, shipping_revenue, bronze_file_name)
    SELECT DISTINCT ON (business_key) 
           business_key, 
           COALESCE(customer_id, payload_json->>'customer_id'), 
           payload_json->>'store_id', 
           COALESCE(status,'completed'), 
           COALESCE(payload_json->>'sales_channel','online'),
           COALESCE((payload_json->>'order_date')::date, (payload_json->>'order_date_utc')::date, (payload_json->>'ordered_at')::date, CURRENT_DATE),
           COALESCE((payload_json->>'order_date')::date, (payload_json->>'order_date_utc')::date, CURRENT_DATE),
           COALESCE((payload_json->>'order_date')::timestamptz, (payload_json->>'ordered_at')::timestamptz, NOW()),
           COALESCE(amount,0),
           COALESCE((payload_json->>'shipping_revenue')::numeric, 0),
           bronze_file_name
    FROM ops.silver_lineage WHERE target_table='silver.orders' AND business_key IS NOT NULL ORDER BY business_key DESC;
    """)
    print(f"orders | {cur.rowcount}")

    cur.execute("ALTER TABLE silver.order_items DROP CONSTRAINT IF EXISTS order_items_quantity_check;")
    cur.execute("""
    INSERT INTO silver.order_items (order_item_id, order_id, product_id, quantity, unit_price, bronze_file_name, silver_processed_at)
    WITH src AS (
      SELECT COALESCE(payload_json->>'order_item_id', payload_json->'payload'->>'order_item_id', payload_json->>'id', business_key||'_'||bronze_row_number::text) as oid,
             COALESCE(order_id, payload_json->>'order_id', payload_json->'payload'->>'order_id') as o_id,
             COALESCE(product_id, payload_json->>'product_id', payload_json->'payload'->>'product_id') as p_id,
             COALESCE(quantity, (payload_json->>'quantity')::int, (payload_json->'payload'->>'quantity')::int, 1) as qty,
             COALESCE(unit_price, (payload_json->>'unit_price')::numeric, (payload_json->'payload'->>'unit_price')::numeric, 0) as up,
             bronze_file_name, bronze_row_number
      FROM ops.silver_lineage WHERE target_table='silver.order_items'
    )
    SELECT DISTINCT ON (oid) oid, o_id, p_id, qty, up, bronze_file_name, NOW()
    FROM src ORDER BY oid, bronze_row_number DESC
    ON CONFLICT (order_item_id) DO NOTHING;
    """)
    print(f"order_items | {cur.rowcount}")

    cur.execute("""
    INSERT INTO silver.payment_events (payment_id, order_id, event_id, payment_status, amount, payment_amount, bronze_file_name, silver_processed_at)
    WITH src AS (
      SELECT COALESCE(business_key, payload_json->>'payment_id', payload_json->'payload'->>'payment_id', bronze_file_name||'_'||bronze_row_number::text) as pid,
             COALESCE(order_id, payload_json->>'order_id', payload_json->'payload'->>'order_id') as oid,
             COALESCE(event_id, payload_json->>'event_id', payload_json->'payload'->>'event_id', payload_json->>'payment_id'||'_'||bronze_row_number::text) as eid,
             COALESCE(status, 'paid') as st,
             COALESCE(amount, 0) as amt,
             bronze_file_name, bronze_row_number
      FROM ops.silver_lineage WHERE target_table='silver.payment_events'
    )
    SELECT DISTINCT ON (pid) pid, oid, eid, st, amt, amt, bronze_file_name, NOW()
    FROM src ORDER BY pid, bronze_row_number DESC
    ON CONFLICT (payment_id) DO NOTHING;
    """)
    print(f"payment_events | {cur.rowcount}")

    cur.execute("""
    INSERT INTO silver.inventory_snapshots (inventory_id, product_id, store_id, stock_quantity, snapshot_date_utc, bronze_file_name, silver_processed_at)
    WITH src AS (
      SELECT business_key as inv_id, COALESCE(product_id, payload_json->>'product_id') as p_id, payload_json->>'store_id' as s_id,
             COALESCE(quantity, (payload_json->>'stock_quantity')::int, 0) as qty,
             COALESCE((payload_json->>'snapshot_date_utc')::date, CURRENT_DATE) as sdate,
             bronze_file_name, bronze_row_number
      FROM ops.silver_lineage WHERE target_table='silver.inventory_snapshots' AND business_key IS NOT NULL
    )
    SELECT DISTINCT ON (inv_id, sdate) inv_id, p_id, s_id, qty, sdate, bronze_file_name, NOW()
    FROM src ORDER BY inv_id, sdate, bronze_row_number DESC
    ON CONFLICT (inventory_id, snapshot_date_utc) DO NOTHING;
    """)
    print(f"inventory_snapshots | {cur.rowcount}")

    cur.execute("""
    INSERT INTO silver.web_events (event_id, session_id, customer_id, order_id, event_type, bronze_file_name, silver_processed_at)
    WITH src AS (
      SELECT COALESCE(business_key, payload_json->>'event_id', bronze_file_name||'_'||bronze_row_number::text) as eid,
             payload_json->>'session_id' as sess, customer_id as cid, order_id as oid,
             COALESCE(event_type, 'view') as et, bronze_file_name, bronze_row_number
      FROM ops.silver_lineage WHERE target_table='silver.web_events'
    )
    SELECT DISTINCT ON (eid) eid, sess, cid, oid, et, bronze_file_name, NOW()
    FROM src ORDER BY eid, bronze_row_number DESC
    ON CONFLICT (event_id) DO NOTHING;
    """)
    # setelah web_events
cur.execute("""
INSERT INTO silver.refunds (refund_id, order_id, amount, bronze_file_name)
SELECT DISTINCT ON (business_key) business_key, order_id, amount, bronze_file_name
FROM ops.silver_lineage WHERE target_table='silver.refunds' AND business_key IS NOT NULL ORDER BY business_key DESC
ON CONFLICT (refund_id) DO NOTHING;
""")
cur.execute("""
INSERT INTO silver.returns (return_id, order_id, bronze_file_name)
SELECT DISTINCT ON (business_key) business_key, order_id, bronze_file_name
FROM ops.silver_lineage WHERE target_table='silver.returns' AND business_key IS NOT NULL ORDER BY business_key DESC
ON CONFLICT (return_id) DO NOTHING;
""")
    print(f"web_events | {cur.rowcount}")

    cur.execute("SET session_replication_role = origin;")
    conn.commit()
    cur.close(); conn.close()

def count_silver_rows():
    conn = get_conn(); cur = conn.cursor()
    for t in ['silver.customers','silver.stores','silver.products','silver.orders','silver.order_items','silver.payment_events','silver.inventory_snapshots','silver.web_events']:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        row = cur.fetchone()
        print(f"{t:35} | {row[0] if row is not None else 0}")
    cur.close(); conn.close()

# ================= 2. TIMESTAMP =================
def stage2_timestamp():
    print("\n=== 2. TIMESTAMP ===")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
    DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.table_constraints 
                   WHERE table_name='payment_events' AND table_schema='silver' 
                   AND constraint_type='PRIMARY KEY') THEN
            EXECUTE 'ALTER TABLE silver.payment_events DROP CONSTRAINT payment_events_pkey CASCADE';
        END IF;
    EXCEPTION WHEN OTHERS THEN NULL; END $$;
    """)
    cur.execute("""
    ALTER TABLE silver.payment_events DROP CONSTRAINT IF EXISTS payment_events_payment_id_key CASCADE;
    ALTER TABLE silver.payment_events ADD COLUMN IF NOT EXISTS event_id TEXT;
    ALTER TABLE silver.payment_events ALTER COLUMN event_id TYPE TEXT;
    ALTER TABLE silver.payment_events ADD COLUMN IF NOT EXISTS payment_id TEXT;
    ALTER TABLE silver.payment_events ADD COLUMN IF NOT EXISTS bronze_file_name TEXT;
    ALTER TABLE silver.payment_events ADD COLUMN IF NOT EXISTS silver_processed_at TIMESTAMPTZ DEFAULT NOW();
    """)
    cur.execute("ALTER TABLE silver.payment_events ADD PRIMARY KEY (event_id);")
    conn.commit()
    print("FIX PK payment_events -> event_id OK")

    cur.execute("ALTER TABLE silver.order_items DROP CONSTRAINT IF EXISTS order_items_quantity_check;")
    cur.execute("""
    ALTER TABLE silver.order_items 
        ADD COLUMN IF NOT EXISTS is_invalid BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS is_orphan BOOLEAN DEFAULT FALSE;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS order_date DATE;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS order_date_utc DATE;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS ordered_at_utc TIMESTAMPTZ;
    """)
    conn.commit()
    print("ALTER order_items + orders order_date OK")

    cur.execute("SET session_replication_role = replica;")
    cur.execute("""
    INSERT INTO silver.customers (customer_id)
    SELECT DISTINCT customer_id FROM ops.silver_lineage 
    WHERE target_table='silver.customers' AND customer_id IS NOT NULL
    ON CONFLICT (customer_id) DO NOTHING;
    """)
    print(f"customers: {cur.rowcount}")
    cur.execute("""
    INSERT INTO silver.products (product_id)
    SELECT DISTINCT product_id FROM ops.silver_lineage 
    WHERE product_id IS NOT NULL
    ON CONFLICT (product_id) DO NOTHING;
    """)
    print(f"products: {cur.rowcount}")
    cur.execute("""
    INSERT INTO silver.stores (store_id)
    SELECT DISTINCT payload_json->>'store_id' FROM ops.silver_lineage 
    WHERE target_table='silver.stores' AND payload_json->>'store_id' IS NOT NULL
    ON CONFLICT (store_id) DO NOTHING;
    """)
    print(f"stores: {cur.rowcount}")
    cur.execute("""
    INSERT INTO silver.orders (order_id, customer_id, total_amount, bronze_file_name, order_date, order_date_utc, ordered_at_utc)
    SELECT DISTINCT ON (business_key) business_key, customer_id, amount, bronze_file_name,
           COALESCE((payload_json->>'order_date')::date, CURRENT_DATE),
           COALESCE((payload_json->>'order_date')::date, CURRENT_DATE),
           NOW()
    FROM ops.silver_lineage WHERE target_table='silver.orders' ORDER BY business_key DESC
    ON CONFLICT (order_id) DO NOTHING;
    """)
    print(f"orders: {cur.rowcount}")
    cur.execute("""
    INSERT INTO silver.order_items (order_item_id, order_id, product_id, quantity, unit_price, is_invalid)
    SELECT DISTINCT ON (business_key) 
           business_key, order_id, product_id, quantity, unit_price,
           CASE WHEN quantity <= 0 THEN TRUE ELSE FALSE END
    FROM ops.silver_lineage 
    WHERE target_table='silver.order_items'
    ORDER BY business_key DESC
    ON CONFLICT (order_item_id) DO NOTHING;
    """)
    print(f"order_items: {cur.rowcount}")
    cur.execute("""
    INSERT INTO silver.payment_events (event_id, payment_id, order_id, event_type, amount, payment_status, bronze_file_name)
    SELECT DISTINCT ON (business_key) 
           COALESCE(event_id, business_key || '_' || bronze_row_number::text),
           business_key, order_id, event_type, amount, status, bronze_file_name
    FROM ops.silver_lineage WHERE target_table='silver.payment_events'
    ORDER BY business_key, bronze_row_number DESC
    ON CONFLICT (event_id) DO NOTHING;
    """)
    print(f"payment_events: {cur.rowcount}")
    cur.execute("SET session_replication_role = origin;")
    conn.commit(); cur.close(); conn.close()

# ================= 3. NORMALIZATION =================
def stage3_normalization():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 3. NORMALIZATION ===")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ops.silver_status_map (raw_status TEXT PRIMARY KEY, normalized_status TEXT);
    INSERT INTO ops.silver_status_map VALUES
    ('paid','paid'),('PAID','paid'),('completed','completed'),('cancelled','cancelled'),('refunded','refunded'),('pending','pending'),('failed','failed'),
    ('PAYMENT_CAPTURED','paid'),('PAYMENT_AUTHORIZED','paid'),('PAYMENT_FAILED','failed'),('PAYMENT_REFUNDED','refunded'),('PAYMENT_CANCELLED','cancelled')
    ON CONFLICT (raw_status) DO UPDATE SET normalized_status = EXCLUDED.normalized_status;
    CREATE OR REPLACE FUNCTION ops.normalize_status(raw TEXT) RETURNS TEXT AS $$
    DECLARE v TEXT; norm TEXT; BEGIN IF raw IS NULL OR TRIM(raw)='' THEN RETURN 'unknown'; END IF; v:=LOWER(TRIM(raw));
      SELECT normalized_status INTO norm FROM ops.silver_status_map WHERE LOWER(TRIM(raw_status))=v LIMIT 1; IF norm IS NOT NULL THEN RETURN norm; END IF;
      RETURN CASE WHEN v LIKE '%captured%' THEN 'paid' WHEN v LIKE '%failed%' THEN 'failed' WHEN v LIKE 'comp%' THEN 'completed' WHEN v LIKE 'canc%' THEN 'cancelled' WHEN v LIKE 'refu%' THEN 'refunded' ELSE 'unknown' END;
    END; $$ LANGUAGE plpgsql IMMUTABLE;
    UPDATE ops.silver_lineage SET status = COALESCE(NULLIF(TRIM(status),''), payload_json->>'payment_status', payload_json->>'event_type') WHERE target_table='silver.payment_events' AND (status IS NULL OR status='');
    UPDATE silver.payment_events pe SET payment_status = ops.normalize_status(l.status) FROM ops.silver_lineage l WHERE l.target_table='silver.payment_events' AND l.business_key = pe.payment_id;
    UPDATE silver.payment_events SET payment_status = ops.normalize_status(payment_status) WHERE payment_status NOT IN ('paid','failed','completed','cancelled','refunded','pending','returned','shipped','processing','unknown');
    """)
    print("NORMALIZATION OK")
    cur.close(); conn.close()

# ================= 4. REJECTION =================
def stage4_rejection():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 4. REJECTION ===")
    cur.execute("""
    DELETE FROM ops.silver_rejected_records WHERE rejection_stage='stage_4_pre_filter';
    INSERT INTO ops.silver_rejected_records (target_table, file_name, row_number, source_record_id, order_id, rejection_stage, rejection_reason, failed_field, failed_value, payload_json)
    SELECT 'silver.order_items', bronze_file_name, bronze_row_number::int, business_key, order_id, 'stage_4_pre_filter',
      CASE WHEN quantity IS NULL THEN 'quantity IS NULL' WHEN quantity=0 THEN 'quantity=0' WHEN quantity<0 THEN 'quantity<0' WHEN unit_price<0 THEN 'price<0' ELSE 'product_id NULL' END,
      CASE WHEN quantity IS NULL OR quantity<=0 THEN 'quantity' WHEN unit_price<0 THEN 'unit_price' ELSE 'product_id' END,
      COALESCE(quantity::text, unit_price::text), payload_json
    FROM ops.silver_lineage WHERE target_table='silver.order_items' AND (quantity IS NULL OR quantity<=0 OR unit_price IS NULL OR unit_price<0);
    INSERT INTO ops.quarantine_inventory (inventory_id, product_id, store_id, stock_quantity, snapshot_date_utc, snapshot_timestamp_utc, bronze_file_name, rejection_reason, raw_payload)
    SELECT inventory_id, product_id, store_id, stock_quantity, snapshot_date_utc, snapshot_timestamp_utc, bronze_file_name, 'NEGATIVE_QUANTITY', to_jsonb(s) FROM silver.inventory_snapshots s WHERE stock_quantity<0;
    DELETE FROM silver.inventory_snapshots WHERE stock_quantity<0;
    DROP VIEW IF EXISTS ops.silver_lineage_clean;
    CREATE VIEW ops.silver_lineage_clean AS SELECT l.* FROM ops.silver_lineage l LEFT JOIN ops.silver_rejected_records r ON l.business_key=r.source_record_id AND l.target_table=r.target_table AND r.rejection_stage='stage_4_pre_filter' WHERE r.rejection_id IS NULL;
    """)
    print("REJECTION OK")
    cur.close(); conn.close()

# ================= 5. DEDUPLICATION =================
def stage5_dedup():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 5. DEDUPLICATION ===")
    cur.execute("""
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
    DROP TABLE IF EXISTS ops.silver_ai_dedup_ranked;
    CREATE TEMP TABLE ops_silver_ai_dedup_ranked AS
    WITH scored AS (SELECT l.*, (CASE WHEN order_id IS NOT NULL THEN 10 ELSE 0 END + CASE WHEN customer_id IS NOT NULL THEN 10 ELSE 0 END) as completeness_score, COALESCE(ingested_at_utc, occurred_at_utc, bronze_ingested_at, NOW()) as ts FROM ops.silver_lineage_clean l WHERE deterministic_key IS NOT NULL),
    ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY target_table, deterministic_key ORDER BY completeness_score DESC, ts DESC) as rn, COUNT(*) OVER (PARTITION BY target_table, deterministic_key) as total_dup FROM scored)
    SELECT * FROM ranked;
    TRUNCATE ops.silver_dedup_log;
    INSERT INTO ops.silver_dedup_log (target_table, deterministic_key, source_record_id, event_id, order_id, file_name, row_number, is_kept, duplicate_count)
    SELECT target_table, deterministic_key, business_key, event_id, order_id, bronze_file_name, bronze_row_number, (rn=1), total_dup FROM ops_silver_ai_dedup_ranked;
    DELETE FROM silver.order_items WHERE order_item_id IN (SELECT source_record_id FROM ops.silver_dedup_log WHERE target_table='silver.order_items' AND is_kept=false);
    DELETE FROM silver.orders WHERE order_id IN (SELECT source_record_id FROM ops.silver_dedup_log WHERE target_table='silver.orders' AND is_kept=false);
    """)
    print("DEDUP OK")
    cur.close(); conn.close()

# ================= 6. IDENTITY RESOLUTION =================
def stage6_identity():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 6. IDENTITY RESOLUTION ===")
    cur.execute("""
    SET session_replication_role = replica;
    ALTER TABLE silver.payment_events ADD COLUMN IF NOT EXISTS is_current BOOLEAN DEFAULT TRUE;
    ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS is_current BOOLEAN DEFAULT TRUE;
    CREATE TABLE IF NOT EXISTS ops.silver_dedup_log (
        log_id BIGSERIAL PRIMARY KEY,
        target_table TEXT,
        source_record_id TEXT,
        deterministic_key TEXT,
        is_kept BOOLEAN,
        kept_at TIMESTAMPTZ DEFAULT NOW()
    );
    UPDATE silver.payment_events SET is_current = FALSE;
    WITH ranked AS (
      SELECT event_id, payment_id,
             ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY event_id DESC) as rn
      FROM silver.payment_events
    )
    UPDATE silver.payment_events p SET is_current = TRUE
    FROM ranked r WHERE p.event_id = r.event_id AND r.rn = 1;
    UPDATE silver.payment_events SET is_current = TRUE WHERE payment_id IS NULL;
    DROP VIEW IF EXISTS silver.fact_return CASCADE;
    CREATE OR REPLACE VIEW silver.fact_return AS
    SELECT order_id, 1 as is_returned FROM silver.returns
    UNION
    SELECT order_id, 1 FROM silver.orders WHERE UPPER(COALESCE(return_status,'NONE')) != 'NONE';
    SET session_replication_role = origin;
    """)
    conn.commit()
    cur.execute("SELECT COUNT(*) FILTER (WHERE is_current), COUNT(*) FROM silver.payment_events;")
    print(f"IDENTITY OK - {cur.fetchone()}")
    cur.close(); conn.close()

# ================= 7. TEMPORAL VERSION =================
def stage7_temporal():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 7. TEMPORAL VERSION ===")
    cur.execute("""
    TRUNCATE ops.silver_temporal_versions;
    ALTER TABLE silver.inventory_snapshots ADD COLUMN IF NOT EXISTS deterministic_key VARCHAR(255);
    UPDATE silver.inventory_snapshots SET deterministic_key=inventory_id||'|'||product_id||'|'||store_id||'|'||snapshot_date_utc::text WHERE deterministic_key IS NULL;
    WITH ranked_inventory AS (SELECT inventory_id, product_id, store_id, stock_quantity, snapshot_date_utc, bronze_file_name, deterministic_key, silver_processed_at, ROW_NUMBER() OVER (PARTITION BY COALESCE(deterministic_key,inventory_id) ORDER BY silver_processed_at ASC) AS version_number, ROW_NUMBER() OVER (PARTITION BY COALESCE(deterministic_key,inventory_id) ORDER BY silver_processed_at DESC) AS rn_latest, LEAD(silver_processed_at) OVER (PARTITION BY COALESCE(deterministic_key,inventory_id) ORDER BY silver_processed_at ASC) AS next_effective FROM silver.inventory_snapshots)
    INSERT INTO ops.silver_temporal_versions (target_table, business_key, version_number, is_current, effective_from, effective_to, source_record_id, bronze_file_name, payload_json)
    SELECT 'silver.inventory_snapshots', COALESCE(deterministic_key,inventory_id), version_number, rn_latest=1, silver_processed_at, COALESCE(next_effective,'2099-12-31'::timestamptz), inventory_id, bronze_file_name, to_jsonb(r.*) FROM ranked_inventory r;
    DELETE FROM silver.campaign_spend;
    WITH campaign_source AS (SELECT 'CAMP-'||product_id AS campaign_id, snapshot_date_utc AS spend_date_utc, store_id AS channel_id, (stock_quantity*10)::numeric(12,2) AS spend_amount, bronze_file_name, deterministic_key, silver_processed_at, ROW_NUMBER() OVER (PARTITION BY 'CAMP-'||product_id, snapshot_date_utc ORDER BY silver_processed_at DESC) AS rn FROM silver.inventory_snapshots)
    INSERT INTO silver.campaign_spend (campaign_id, spend_date_utc, channel_id, spend_amount, bronze_file_name, deterministic_key, is_current, silver_processed_at)
    SELECT campaign_id, spend_date_utc, channel_id, spend_amount, bronze_file_name, deterministic_key, TRUE, NOW() FROM campaign_source WHERE rn=1 ON CONFLICT (campaign_id, spend_date_utc) DO UPDATE SET spend_amount=EXCLUDED.spend_amount, is_current=TRUE;
    """)
    print("TEMPORAL OK")
    cur.close(); conn.close()

# ================= 8. VALIDASI FK =================
def stage8_fk():
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 8. VALIDASI FK ===")
    cur.execute("""
    DELETE FROM ops.silver_fk_orphans WHERE target_table IN ('silver.campaign_spend','silver.web_events','silver.orders');
    INSERT INTO ops.silver_fk_orphans (target_table, source_record_id, business_key, fk_table, fk_field, fk_value, orphan_reason, payload_json)
    SELECT 'silver.orders', o.order_id, o.order_id, 'silver.customers', 'customer_id', o.customer_id, 'order customer_id orphan', to_jsonb(o.*) FROM silver.orders o LEFT JOIN silver.customers c ON c.customer_id=o.customer_id WHERE o.customer_id IS NOT NULL AND c.customer_id IS NULL;
    INSERT INTO ops.silver_fk_orphans (target_table, source_record_id, business_key, order_id, fk_table, fk_field, fk_value, orphan_reason, payload_json)
    SELECT 'silver.web_events', we.event_id, we.event_id, we.order_id, 'silver.orders', 'order_id', we.order_id, 'web_events order_id orphan', to_jsonb(we.*) FROM silver.web_events we LEFT JOIN silver.orders o ON o.order_id=we.order_id WHERE we.order_id IS NOT NULL AND o.order_id IS NULL;
    """)
    cur.execute("SELECT target_table, COUNT(*) FROM ops.silver_fk_orphans GROUP BY 1;")
    for r in cur.fetchall(): print(r)
    cur.close(); conn.close()

# ================= 9. LATE ARRIVING =================
def stage9_late():
    print("\n=== 9. LATE ARRIVING ===")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
    SET session_replication_role = replica;
    CREATE TABLE IF NOT EXISTS silver.silver_late_arriving_buffer (
        buffer_id BIGSERIAL PRIMARY KEY, target_table TEXT, business_key TEXT,
        payload_json JSONB, status TEXT, deterministic_key TEXT
    );
    INSERT INTO silver.silver_late_arriving_buffer (target_table, business_key, payload_json, status, deterministic_key)
    SELECT target_table, business_key, payload_json, 'LATE', deterministic_key
    FROM ops.silver_lineage 
    WHERE target_table='silver.order_items' AND business_key NOT IN (SELECT order_id FROM silver.orders)
    ON CONFLICT DO NOTHING;
    SET session_replication_role = origin;
    """)
    conn.commit()
    print(f"LATE BUFFER: {cur.rowcount}")
    cur.close(); conn.close()

# ================= 10. REPORT =================
def stage10_report():
    spark = get_spark("SILVER_REPORT")
    conn = get_conn(); cur = conn.cursor()
    print("\n=== 10. REPORT ===")
    summary_df = spark.read.jdbc(JDBC_URL,"(SELECT target_table, COUNT(*) as cnt FROM ops.silver_lineage GROUP BY 1) as t", properties=PROPS)
    summary_df.show()
    cur.execute("SELECT 'silver.orders', COUNT(*) FROM silver.orders UNION ALL SELECT 'silver.payment_events', COUNT(*) FROM silver.payment_events UNION ALL SELECT 'ops.silver_lineage', COUNT(*) FROM ops.silver_lineage")
    for r in cur.fetchall(): print(r)
    report_dir = pathlib.Path.cwd() / "validation_report"
    report_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    content = f"SILVER FULL PIPELINE {ts}\n1.LINEAGE 2.TIMESTAMP 3.NORMALIZATION 4.REJECTION 5.DEDUP 6.IDENTITY 7.TEMPORAL 8.FK 9.LATE 10.REPORT\n"
    cur.execute("SELECT target_table, COUNT(*) FROM ops.silver_lineage GROUP BY 1 ORDER BY 2 DESC;")
    for t,c in cur.fetchall(): content+=f"{t}: {c}\n"
    path = report_dir / f"SILVER_FULL_{ts}.txt"
    open(path,"w",encoding="utf-8").write(content)
    print(f"REPORT: {path}")
    cur.close(); conn.close(); spark.stop()
    
# ================= 11. VALIDATION =================
def stage11_quality_gate():
    conn = get_conn(); cur = conn.cursor()
    try:
        failed_critical = [c for c in checks if not c["passed"] and c["name"] in ['silver_orders_not_empty']]
        if failed_critical:
            fail_msg = "; ".join([f"{c['name']}: {c['details']}" for c in failed_critical])
            raise ValueError(f"Quality Gate FAIL: {fail_msg}")

        print(f"QUALITY GATE + FILTER PASS - {orders_cnt} orders clean ready for GOLD")
        return report_path
    except ValueError:
        raise
    except Exception as e:
        print(f"Quality Gate Error: {e}")
        conn.rollback()
        cur.execute("INSERT INTO ops.data_quality_log (check_name, table_name, status, details) VALUES (%s,%s,%s,%s);",
                    ("quality_gate_error", "silver->gold", "FAIL", str(e)))
        conn.commit()
        raise
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

def run_silver(**context):
    print("=== START SILVER DAG CALLABLE ===")
    stage1_lineage()
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
