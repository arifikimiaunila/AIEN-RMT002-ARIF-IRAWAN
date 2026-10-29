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

def stage9_fk():
    print("\n=== 9. VALIDASI FK & ORPHANS (SILVER.ORDER_ITEMS) ===")
    
    sql_query = """
    -- =========================================================================
    -- ETL PIPELINE: BRONZE TO SILVER (FK ORPHAN DETECTION & HANDLING)
    -- =========================================================================
    WITH prepared_order_items AS (
        -- Langkah 1: Ambil data mentah order_items dari bronze.raw_data yang berstatus 'NEW'
        SELECT 
            b.bronze_id,
            b.order_item_id,
            b.order_id,
            b.product_id,
            COALESCE(b.payload->>'promotion_id', NULL) AS promotion_id,
            COALESCE(b.quantity, (b.payload->>'quantity')::numeric, 1) AS quantity,
            COALESCE(b.unit_price, (b.payload->>'unit_price')::numeric, 0) AS unit_price,
            COALESCE((b.payload->>'item_discount_amount')::numeric, 0) AS item_discount_amount,
            b.payload,
            b.source_record_id,
            b.file_name,
            b.row_number,
            b.checksum,
            b.ingested_at,
            b.target_table,
            md5(COALESCE(b.order_item_id, '') || '|' || COALESCE(b.order_id, '') || '|' || COALESCE(b.product_id, '')) AS computed_deterministic_key
        FROM bronze.raw_data b
        WHERE b.processing_status = 'NEW'
          AND b.target_table = 'silver.order_items'
          AND b.order_item_id IS NOT NULL
    ),
    checked_orphans AS (
        -- Langkah 2: Lakukan pemeriksaan Foreign Key (FK) terhadap tabel master induk (silver.orders)
        SELECT 
            poi.*,
            CASE 
                WHEN o.order_id IS NULL THEN TRUE
                ELSE FALSE
            END AS is_orphan_record,
            CASE 
                WHEN o.order_id IS NULL THEN 'Parent order_id does not exist in silver.orders'
                ELSE NULL
            END AS orphan_reason_text
        FROM prepared_order_items poi
        LEFT JOIN silver.orders o ON poi.order_id = o.order_id
    ),
    insert_orphans_log AS (
        -- Langkah 3A: Catat data orphan ke tabel operasional ops.silver_fk_orphans
        INSERT INTO ops.silver_fk_orphans (
            target_table, source_record_id, business_key, order_id, 
            product_id, fk_table, fk_field, fk_value, 
            orphan_reason, is_resolved, payload_json, detected_at,
            item_discount_amount, sales_channel
        )
        SELECT 
            'silver.order_items', source_record_id, order_item_id, order_id,
            product_id, 'silver.orders', 'order_id', order_id,
            orphan_reason_text, FALSE, payload, CURRENT_TIMESTAMP,
            item_discount_amount, 'online'
        FROM checked_orphans
        WHERE is_orphan_record = TRUE
        RETURNING orphan_id
    ),
    insert_to_pending AS (
        -- Langkah 3B: Masukkan data orphan ke tabel buffer pending (`silver.order_items_pending`)
        INSERT INTO silver.order_items_pending (
            order_item_id, order_id, product_id, quantity, 
            unit_price, item_discount_amount, status, is_orphan,
            source_row_id, bronze_file_name, bronze_row_number,
            bronze_source_record_id, bronze_checksum, ingested_at, deterministic_key
        )
        SELECT 
            order_item_id, order_id, product_id, quantity,
            unit_price, item_discount_amount, 'PENDING_PARENT', TRUE,
            source_record_id, file_name, row_number,
            source_record_id, checksum, CURRENT_TIMESTAMP, computed_deterministic_key
        FROM checked_orphans
        WHERE is_orphan_record = TRUE
        ON CONFLICT (order_item_id) DO UPDATE 
        SET 
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            item_discount_amount = EXCLUDED.item_discount_amount,
            status = 'PENDING_PARENT'
        RETURNING order_item_id
    ),
    valid_order_items AS (
        -- Langkah 4: Pisahkan data valid yang induknya sudah tersedia
        SELECT 
            co.order_item_id,
            co.order_id,
            co.product_id,
            co.promotion_id,
            co.quantity,
            co.unit_price,
            co.item_discount_amount,
            (co.quantity * co.unit_price - co.item_discount_amount) AS subtotal,
            'online' AS sales_channel,
            co.source_record_id,
            co.file_name AS bronze_file_name,
            co.row_number AS bronze_row_number,
            co.checksum AS bronze_checksum,
            co.computed_deterministic_key AS deterministic_key
        FROM checked_orphans co
        WHERE co.is_orphan_record = FALSE
    )
    -- Langkah 5: Masukkan data valid ke tabel utama `silver.order_items`
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
        sales_channel, source_record_id, bronze_file_name, bronze_row_number,
        source_record_id, bronze_checksum, CURRENT_TIMESTAMP,
        FALSE, deterministic_key
    FROM valid_order_items
    ON CONFLICT (order_item_id) DO UPDATE 
    SET 
        order_id = EXCLUDED.order_id,
        product_id = EXCLUDED.product_id,
        promotion_id = EXCLUDED.promotion_id,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        item_discount_amount = EXCLUDED.item_discount_amount,
        subtotal = EXCLUDED.subtotal,
        sales_channel = EXCLUDED.sales_channel,
        silver_processed_at = CURRENT_TIMESTAMP,
        is_orphan = FALSE,
        deterministic_key = EXCLUDED.deterministic_key;
    """

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        print("🧹 Membersihkan log FK orphans lama...")
        cur.execute("TRUNCATE ops.silver_fk_orphans;")
        
        print("⏳ Mengeksekusi validasi Foreign Key & penanganan orphan...")
        cur.execute(sql_query)
        conn.commit()
        
        print("📊 Ringkasan Orphan Terdeteksi:")
        cur.execute("SELECT target_table, COUNT(*) FROM ops.silver_fk_orphans GROUP BY 1;")
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"   - Target: {r[0]} | Jumlah Orphan: {r[1]}")
        else:
            print("   - Tidak ada data orphan yang ditemukan.")
            
        cur.execute("SELECT COUNT(*) FROM silver.order_items;")
        print(f"✅ STAGE 9 FK OK. Total baris pada `silver.order_items`: {cur.fetchone()[0]}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"🚨 Error pada stage9_fk: {e}")
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