import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
import json
import hashlib
import pandas as pd
import psycopg2

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
            return conn
        except: 
            continue
    conn = psycopg2.connect(host="postgres", port=5432, dbname="retail_analytics", user="retail", password="retail")
    conn.autocommit = True
    return conn

DEFAULT_FILES = [
    "operational/customers.json", "operational/customer_addresses.json",
    "operational/customer_profiles.json", "operational/products.json", 
    "operational/product_categories.json",
    "operational/stores.json", "operational/sales_channels.json",
    "operational/promotions.json", "operational/orders.json",
    "operational/order_items.json", "operational/order_promotions.json",
    "events/payment_events.json", "events/refund_events.json", "events/return_events.json",
    "events/support_events.json", "events/web_events.json", "inventory/inventory_snapshots.csv",
    "reference/campaign_spend.csv", "reference/city_reference.json"
]

def fill_raw_data():
    """
    Memproses file mentah (CSV/JSON) menggunakan Pandas dan langsung 
    memasukkannya ke tabel PostgreSQL bronze.raw_data.
    """
    conn = get_conn()
    total_rows = 0

    for file in DEFAULT_FILES:
        # Cek path file yang sesuai di dalam container Airflow
        path = f"/opt/airflow/data/raw/{file}"
        if not os.path.exists(path):
            path = f"data/raw/{file}"

        base_file_name = os.path.basename(file)
        nama, ext = os.path.splitext(base_file_name)
        print(f">> Memproses file: {file} (Tabel: {nama})")

        try:
            # 1. Baca file menggunakan Pandas
            if ext == ".csv":
                df = pd.read_csv(path)
            else:
                # Menangani JSON (baik line-delimited maupun standard multiline)
                try:
                    df = pd.read_json(path, lines=True)
                except ValueError:
                    df = pd.read_json(path)

            if df.empty:
                print(f">> File {base_file_name} kosong, melewati...")
                continue

            # 2. Siapkan struktur data untuk tabel bronze.raw_data
            rows_to_insert = []
            for idx, row in df.iterrows():
                row_num = idx + 1
                # Ubah baris Pandas menjadi dictionary lalu ke string JSON (payload)
                payload_json = json.dumps(row.dropna().to_dict(), default=str)
                # Buat SHA-256 checksum dari payload
                checksum = hashlib.sha256(payload_json.encode('utf-8')).hexdigest()

                rows_to_insert.append((
                    base_file_name,
                    row_num,
                    "retail_generator",
                    "NEW",
                    payload_json,
                    checksum
                ))

            # 3. Batch Insert ke PostgreSQL menggunakan psycopg2 (eksekusi cepat)
            cur = conn.cursor()
            insert_query = """
                INSERT INTO bronze.raw_data (file_name, row_number, source_system, processing_status, payload, checksum)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cur.executemany(insert_query, rows_to_insert)
            cur.close()

            file_total = len(rows_to_insert)
            total_rows += file_total
            print(f">> Berhasil memasukkan {file_total} baris dari {base_file_name} ke bronze.raw_data\n")

        except Exception as e:
            print(f">> ERROR saat memproses {file}: {e}\n")

    conn.close()
    return total_rows

def run_bronze():
    conn = get_conn()
    cur = conn.cursor()
    
    # Bersihkan trigger immutable lama jika ada
    for q in [
        "DROP TRIGGER IF EXISTS enforce_immutable_raw_data ON bronze.raw_data;",
    ]:
        try: 
            cur.execute(q)
            conn.commit()
        except: 
            conn.rollback()
    cur.close()
    conn.close()
    print(">> Bronze pre-clean done (keep STABLE trigger)")

    # Jalankan proses pengisian data dengan Pandas
    total = fill_raw_data()

    # Tampilkan breakdown hasil akhir
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT file_name, COUNT(*) FROM bronze.raw_data GROUP BY 1 ORDER BY 2 DESC;")
    print("\n=== FINAL BREAKDOWN ===")
    for fn, cnt in cur.fetchall(): 
        print(f"{cnt:>6} | {fn}")
    cur.close()
    conn.close()
    
    print(f"\nDONE Bronze {total} rows")
    return total

def build_bronze_dag_callable(**context):
    return run_bronze()
