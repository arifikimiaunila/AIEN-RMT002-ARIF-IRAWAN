# ======================================================
# build_bronze.py - FINAL V4.5 - FIX manifest + conn
# ======================================================
import os
import json
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ.get('PATH')}"

from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, coalesce, lit, current_timestamp, input_file_name, monotonically_increasing_id, row_number, sha2, struct, to_json, regexp_extract, concat_ws  # type: ignore[reportMissingImports]
from pyspark.sql.window import Window  
import psycopg2
import psycopg2.extras

try:
    _existing_session = SparkSession.getActiveSession()
    if _existing_session is not None:
        _existing_session.stop()
except Exception:
    pass

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Bronze_FINAL_V4_5")
    .config("spark.driver.host", "localhost")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    .getOrCreate()
)

print(">> Migrasi bronze...")
from airflow.hooks.base import BaseHook

def get_conn():
    for conn_id in ["postgres", "postgres_default", "airflow_db"]:
        try:
            from airflow.hooks.base import BaseHook
            c = BaseHook.get_connection(conn_id)
            conn = psycopg2.connect(
                host=c.host or "postgres",
                port=c.port or 5432,
                dbname=c.schema or "retail_analytics",
                user=c.login or "retail",
                password=c.password or "retail"
            )
            conn.autocommit = True
            print(f">> Connected via {conn_id}")
            return conn
        except Exception:
            continue
    print(">> Fallback hardcoded postgres")
    conn = psycopg2.connect(host="postgres", port=5432, dbname="retail_analytics", user="retail", password="retail")
    conn.autocommit = True
    return conn

# --- INIT BRONZE SCHEMA + FUNCTIONS ---
conn = get_conn()
cur = conn.cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
cur.execute("""
CREATE OR REPLACE FUNCTION bronze.immutable_timestamptz(t TEXT) RETURNS TIMESTAMPTZ AS $$ SELECT t::TIMESTAMPTZ $$ LANGUAGE sql IMMUTABLE;
CREATE OR REPLACE FUNCTION bronze.immutable_date(t TEXT) RETURNS DATE AS $$ SELECT t::DATE $$ LANGUAGE sql IMMUTABLE;
CREATE OR REPLACE FUNCTION bronze.immutable_numeric(t TEXT) RETURNS NUMERIC AS $$ SELECT t::NUMERIC $$ LANGUAGE sql IMMUTABLE;
CREATE OR REPLACE FUNCTION bronze.immutable_int(t TEXT) RETURNS INT AS $$ SELECT t::INT $$ LANGUAGE sql IMMUTABLE;
""")
conn.commit()

# Generated columns - amanin biar gak fail
for q in [
    "ALTER TABLE bronze.ingested_data_events ADD COLUMN IF NOT EXISTS event_id TEXT GENERATED ALWAYS AS (payload->>'event_id') STORED;",
    "ALTER TABLE bronze.ingested_data_events ADD COLUMN IF NOT EXISTS inventory_id TEXT GENERATED ALWAYS AS (payload->>'inventory_id') STORED;",
    "ALTER TABLE bronze.ingested_data_events ADD COLUMN IF NOT EXISTS stock_quantity INT GENERATED ALWAYS AS (bronze.immutable_int(payload->>'stock_quantity')) STORED;",
    "ALTER TABLE bronze.ingested_data_events ADD COLUMN IF NOT EXISTS snapshot_date_utc DATE GENERATED ALWAYS AS (bronze.immutable_date(payload->>'snapshot_date_utc')) STORED;",
    "ALTER TABLE bronze.ingested_data_events ADD COLUMN IF NOT EXISTS product_id TEXT GENERATED ALWAYS AS (payload->>'product_id') STORED;",
    "ALTER TABLE bronze.ingested_data_events ADD COLUMN IF NOT EXISTS store_id TEXT GENERATED ALWAYS AS (payload->>'store_id') STORED;",
]:
    try:
        cur.execute(q); conn.commit()
    except Exception as e:
        print(f"Skip alter: {e}")
        conn.rollback()

try:
    cur.execute("TRUNCATE bronze.payment_events RESTART IDENTITY CASCADE;")
    conn.commit()
except: conn.rollback()
try:
    cur.execute("TRUNCATE bronze.ingested_data_events RESTART IDENTITY CASCADE;")
    conn.commit()
except: conn.rollback()

cur.close(); conn.close()
print(">> Bronze truncated")

def resolve(p):
    for cand in [p, os.path.join("data/raw", p), os.path.join("data", p), os.path.basename(p), p.replace("data/raw/","")]:
        if os.path.exists(cand): return cand
    target = os.path.basename(p)
    for r,_,fs in os.walk("."):
        if target in fs: return os.path.join(r, target)
    return None

def ingest(file_path, table, fmt, ids):
    rp = resolve(file_path)
    if not rp or not os.path.exists(rp):
        print(f"SKIP {file_path} -> not found"); return 0
    print(f"Ingest: {rp} -> bronze.{table}")
    try:
        if "inventory_snapshots" in rp.lower():
            df_raw = spark.read.option("header","true").csv(rp)
            df_raw = df_raw.toDF(*[c.strip().lower() for c in df_raw.columns])
            df = df_raw.select(
                col("product_id"), col("location_id").alias("store_id"),
                col("snapshot_date").alias("snapshot_date_utc"),
                col("available_quantity").cast("int").alias("stock_quantity"),
                col("reserved_quantity").cast("int").alias("reserved_quantity"),
                col("unit_cost").cast("double").alias("unit_cost")
            ).withColumn("inventory_id", concat_ws("_", col("product_id"), col("store_id"), col("snapshot_date_utc"))).withColumn("event_type", lit("inventory_snapshot"))
        else:
            df = spark.read.option("multiline","true").json(rp) if fmt=="json" else spark.read.option("header","true").csv(rp)
            df = df.toDF(*[c.strip() for c in df.columns])

        src_id = coalesce(*[col(c).cast("string") for c in ids if c in df.columns]) if any(c in df.columns for c in ids) else lit("UNKNOWN")
        w = Window.partitionBy(input_file_name()).orderBy(monotonically_increasing_id())
        bronze_df = df.withColumn("file_name", regexp_extract(input_file_name(), r".*/([^/]+)$", 1))\
            .withColumn("row_number", row_number().over(w))\
            .withColumn("source_record_id", src_id)\
            .withColumn("checksum", sha2(to_json(struct([col(c) for c in df.columns])), 256))\
            .withColumn("ingested_at", current_timestamp())\
            .withColumn("payload", to_json(struct([col(c) for c in df.columns])))\
            .select("file_name","row_number","source_record_id","checksum","ingested_at","payload")
        
        rows = bronze_df.collect()
        conn2 = get_conn(); cur2 = conn2.cursor()
        tuples = [(r["file_name"], int(r["row_number"]), str(r["source_record_id"]), r["checksum"], str(r["ingested_at"]), r["payload"]) for r in rows]
        psycopg2.extras.execute_values(cur2, f"INSERT INTO bronze.{table} (file_name,row_number,source_record_id,checksum,ingested_at,payload) VALUES %s", tuples, page_size=1000)
        conn2.commit(); cur2.close(); conn2.close()
        print(f"OK {len(tuples)} rows from {os.path.basename(rp)}"); return len(tuples)
    except Exception as e:
        print(f"Gagal {file_path}: {e}"); import traceback; traceback.print_exc(); return 0

# 1. Manifest
MANIFEST_PATH = resolve("data/raw/manifest.json") or resolve("manifest.json")
manifest_files = []
if MANIFEST_PATH and os.path.exists(MANIFEST_PATH):
    print(f">> Found manifest: {MANIFEST_PATH}")
    try:
        with open(MANIFEST_PATH) as f:
            m = json.load(f)
            if isinstance(m, dict):
                manifest_files = m.get("files", m.get("tables", m.get("sources", [])))
            elif isinstance(m, list):
                manifest_files = m
        ingest(MANIFEST_PATH, "ingested_data_events", "json", ["file_name"])
    except Exception as e:
        print(f"Gagal baca manifest: {e}")

DEFAULT_FILES = [
    ("operational/customers.json","ingested_data_events","json",["customer_id"]),
    ("operational/customer_profiles.json","ingested_data_events","json",["customer_id"]),
    ("operational/customer_addresses.json","ingested_data_events","json",["address_id"]),
    ("operational/products.json","ingested_data_events","json",["product_id"]),
    ("operational/product_categories.json","ingested_data_events","json",["category_id"]),
    ("operational/stores.json","ingested_data_events","json",["store_id"]),
    ("operational/sales_channels.json","ingested_data_events","json",["channel_id"]),
    ("operational/promotions.json","ingested_data_events","json",["promotion_id"]),
    ("operational/orders.json","ingested_data_events","json",["order_id"]),
    ("operational/order_items.json","ingested_data_events","json",["order_item_id"]),
    ("operational/order_promotions.json","ingested_data_events","json",["order_id"]),
    ("events/payment_events.json","payment_events","json",["event_id"]),
    ("events/refund_events.json","ingested_data_events","json",["refund_id"]),
    ("events/return_events.json","ingested_data_events","json",["return_id"]),
    ("events/support_events.json","ingested_data_events","json",["ticket_id"]),
    ("events/web_events.json","ingested_data_events","json",["event_id"]),
    ("inventory/inventory_snapshots.csv","ingested_data_events","csv",["inventory_id"]),
    ("reference/campaign_spend.csv","ingested_data_events","csv",["campaign_id"]),
    ("reference/city_reference.json","ingested_data_events","json",["city_id"]),
]

def run_bronze():
    total=0
    for path, table, fmt, ids in DEFAULT_FILES:
        total+=ingest(path, table, fmt, ids)
    
    conn3=get_conn(); cur3=conn3.cursor()
    cur3.execute("SELECT file_name, COUNT(*) FROM bronze.ingested_data_events GROUP BY 1 ORDER BY 2 DESC;")
    print("\n=== FINAL BREAKDOWN ===")
    for fn,cnt in cur3.fetchall(): print(f"{cnt:>6} | {fn}")
    cur3.execute("SELECT COUNT(*) FROM bronze.payment_events;")
    row = cur3.fetchone()
    print(f"payment_events: {row[0] if row is not None else 0}")
    cur3.close(); conn3.close()
    print(f"\nDONE Bronze {total} rows")
    return total

def build_bronze_dag_callable(**context):
    return run_bronze()

def main(**context):
    return run_bronze()

if __name__ == "__main__":
    run_bronze()
