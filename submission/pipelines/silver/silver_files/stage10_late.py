import traceback
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

def stage10_late():
    print("\n=== 10. LATE ARRIVING & REPROCESSING (SILVER.ORDER_ITEMS_PENDING) ===")
    
    sql_query = """
    -- =========================================================================
    -- ETL PIPELINE: BRONZE TO SILVER (LATE-ARRIVING DATA HANDLING)
    -- =========================================================================
    WITH resolved_late_items AS (
        -- Langkah 1: Saring data dari tabel pending yang induknya (orders) sekarang sudah tersedia di silver
        SELECT 
            p.order_item_id,
            p.order_id,
            p.product_id,
            NULL::TEXT AS promotion_id,
            p.quantity,
            p.unit_price,
            p.item_discount_amount,
            (p.quantity * p.unit_price - p.item_discount_amount) AS subtotal,
            'online' AS sales_channel,
            p.source_row_id,
            p.bronze_file_name,
            p.bronze_row_number,
            p.bronze_source_record_id,
            p.bronze_checksum,
            p.deterministic_key
        FROM silver.order_items_pending p
        INNER JOIN silver.orders o ON p.order_id = o.order_id
        WHERE p.is_orphan = TRUE 
          AND p.status = 'PENDING_PARENT'
    ),
    insert_to_main_table AS (
        -- Langkah 2: Pindahkan data late-arriving yang sudah lengkap ke tabel utama `silver.order_items`
        INSERT INTO silver.order_items (
            order_item_id, order_id, product_id, promotion_id,
            quantity, unit_price, item_discount_amount, subtotal,
            sales_channel, source_row_id, bronze_file_name, bronze_row_number,
            bronze_source_record_id, bronze_checksum, silver_processed_at,
            is_orphan, deterministic_key
        )
        SELECT 
            order_item_id, order_id, product_id, promotion_id,
            quantity, unit_price, item_discount_amount, subtotal,
            sales_channel, source_row_id, bronze_file_name, bronze_row_number,
            bronze_source_record_id, bronze_checksum, CURRENT_TIMESTAMP,
            FALSE, deterministic_key
        FROM resolved_late_items
        ON CONFLICT (order_item_id) DO UPDATE 
        SET 
            order_id = EXCLUDED.order_id,
            product_id = EXCLUDED.product_id,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            item_discount_amount = EXCLUDED.item_discount_amount,
            subtotal = EXCLUDED.subtotal,
            silver_processed_at = CURRENT_TIMESTAMP,
            is_orphan = FALSE,
            deterministic_key = EXCLUDED.deterministic_key
        RETURNING order_item_id
    ),
    log_late_resolution AS (
        -- Langkah 3: Catat riwayat resolusi ke tabel buffer operasional `ops.silver_late_arriving_buffer`
        INSERT INTO ops.silver_late_arriving_buffer (
            target_table, child_table, child_key, order_id, 
            parent_table, parent_key, parent_key_value, status, 
            resolved_at, item_discount_amount, sales_channel, reason
        )
        SELECT 
            'silver.order_items', 'silver.order_items_pending', rli.order_item_id, rli.order_id,
            'silver.orders', 'order_id', rli.order_id, 'RESOLVED',
            CURRENT_TIMESTAMP, rli.item_discount_amount, rli.sales_channel, 
            'Parent order arrived late and successfully resolved'
        FROM resolved_late_items rli
        INNER JOIN insert_to_main_table im ON rli.order_item_id = im.order_item_id
        RETURNING buffer_id
    )
    -- Langkah 4: Hapus data yang telah berhasil diselesaikan dari tabel pending `silver.order_items_pending`
    DELETE FROM silver.order_items_pending p
    USING resolved_late_items rli
    WHERE p.order_item_id = rli.order_item_id;
    """

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        print("⏳ Mengeksekusi proses resolusi data late-arriving...")
        cur.execute(sql_query)
        conn.commit()
        
        print("📊 Statistik Akhir Pipeline:")
        cur.execute("SELECT COUNT(*) FROM silver.payment_events;")
        print(f"   - Total baris `silver.payment_events`: {cur.fetchone()[0]}")
        
        cur.execute("SELECT COUNT(*) FROM silver.orders;")
        print(f"   - Total baris `silver.orders`: {cur.fetchone()[0]}")
        
        print("✅ LATE ARRIVING & REPROCESSING OK.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"🚨 Error pada stage10_late: {e}")
        traceback.print_exc()
        raise

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
        print("=" * 60 + "\n")