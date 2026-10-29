import psycopg2
import traceback

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

def count_silver_rows(fail_on_empty=True):
    """
    Cek semua tabel silver. Jika ada tabel yang kosong (0 baris) atau NULL 
    akibat kegagalan pengisian dari bronze, picu alarm keras dan gagalkan DAG.
    """
    tables_to_check = [
        'silver.customers',
        'silver.customer_addresses',
        'silver.customer_profiles',
        'silver.city_reference',
        'silver.products',
        'silver.product_categories',
        'silver.stores',
        'silver.sales_channels',
        'silver.orders',
        'silver.order_items',
        'silver.order_promotions',
        'silver.payment_events',
        'silver.refund_events',
        'silver.return_events',
        'silver.support_events',
        'silver.web_events',
        'silver.inventory_snapshots',
        'silver.campaign_spend',
        'silver.promotions'
    ]
    
    failed_tables = []
    total_rows = 0
    
    print("\n" + "=" * 60)
    print("SILVER ROW COUNT CHECK + ALARM")
    print("=" * 60)
    
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        for t in tables_to_check:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                row = cur.fetchone()
                count = row[0] if row is not None else 0
                total_rows += count
                
                if count is None or count == 0:
                    print(f"🚨 ALARM NULL/KOSONG | {t:35} | {count} rows -> GAGAL PENGISIAN!")
                    failed_tables.append(t)
                else:
                    print(f"✅ OK               | {t:35} | {count} rows")
                    
            except Exception as e:
                # Tabel belum ada atau error akses dianggap gagal
                print(f"🚨 ALARM MISSING      | {t:35} | ERROR: {e}")
                failed_tables.append(f"{t} (missing/error)")

    except Exception as db_err:
        print(f"🚨 Gagal terhubung ke database saat pengecekan row count: {db_err}")
        traceback.print_exc()
        if fail_on_empty:
            raise
        return False
        
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    print("=" * 60)
    print(f"TOTAL SILVER ROWS: {total_rows}")
    print("=" * 60)
    
    if failed_tables:
        msg = f"🚨🚨 ALARM: {len(failed_tables)} tabel silver KOSONG/NULL karena gagal pengisian bronze: {', '.join(failed_tables)}"
        print(msg)
        print("Saran: Cek kembali proses ingestion data mentah pada tabel bronze.")
        if fail_on_empty:
            raise ValueError(msg)
        return False
    else:
        print("✅ SEMUA TABEL SILVER TERISI - AMAN")
        print("=" * 60 + "\n")
        return True
