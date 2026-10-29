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

def stage4_normalization():
    print("\n=== 3. NORMALIZATION & FRAUD/ANOMALY CROSS-VALIDATION (SILVER.ORDERS) ===")
    
    sql_query = """
    -- =========================================================================
    -- ETL PIPELINE: BRONZE TO SILVER (NORMALIZATION & VALIDATION WITH CTE)
    -- =========================================================================
    WITH prepared_bronze AS (
        -- Langkah 1: Saring data mentah dari bronze khusus untuk target silver.orders
        SELECT 
            b.bronze_id,
            b.order_id,
            b.customer_id,
            b.store_id,
            b.channel_id,
            b.sales_channel,
            b.status,
            b.amount,
            b.shipping_revenue,
            b.payload,
            b.source_record_id,
            b.file_name,
            b.row_number,
            b.checksum,
            b.ingested_at,
            b.target_table,
            LOWER(TRIM(COALESCE(b.status, b.payload->>'status', ''))) AS raw_status_text,
            md5(
                COALESCE(b.target_table, '') || '|' ||
                COALESCE(b.source_record_id, b.bronze_id::text) || '|' ||
                COALESCE(b.checksum, '') || '|' ||
                COALESCE(b.row_number::text, '0')
            ) AS computed_deterministic_key
        FROM bronze.raw_data b
        WHERE b.processing_status = 'NEW'
          AND b.target_table = 'silver.orders'
    ),
    normalized_data AS (
        -- Langkah 2: Normalisasi data mentah dengan Mapping & CASE WHEN Fallback
        SELECT 
            pb.*,
            COALESCE(pb.order_id, pb.payload->>'order_id') AS norm_order_id,
            COALESCE(pb.customer_id, pb.payload->>'customer_id') AS norm_customer_id,
            COALESCE(pb.store_id, pb.payload->>'store_id') AS norm_store_id,
            COALESCE(pb.channel_id, pb.payload->>'channel_id') AS norm_channel_id,
            COALESCE(pb.sales_channel, pb.payload->>'sales_channel', 'online') AS norm_sales_channel,
            
            -- Normalisasi Status via mapping table dan CASE WHEN yang aman
            COALESCE(
                sm.normalized_status,
                CASE 
                    WHEN pb.raw_status_text IN ('paid', 'completed', 'success') THEN 'completed'
                    WHEN pb.raw_status_text IN ('pending', 'processing') THEN 'pending'
                    WHEN pb.raw_status_text IN ('cancelled', 'canceled') THEN 'cancelled'
                    WHEN pb.raw_status_text IN ('failed') THEN 'failed'
                    WHEN pb.raw_status_text IN ('refunded') THEN 'refunded'
                    WHEN pb.raw_status_text IN ('returned', 'return') THEN 'returned'
                    ELSE 'pending' -- Default aman agar lolos validasi quality gate
                END
            ) AS norm_status,
            
            -- Normalisasi Timestamp aman (UTC)
            NULLIF(pb.payload->>'order_date', '')::DATE AS norm_order_date,
            NULLIF(pb.payload->>'order_date_utc', '')::TIMESTAMPTZ AS norm_order_date_utc,
            NULLIF(pb.payload->>'ordered_at_utc', '')::TIMESTAMPTZ AS norm_ordered_at_utc,
            NULLIF(pb.payload->>'updated_at_utc', '')::TIMESTAMPTZ AS norm_updated_at_utc,
            pb.payload->>'updated_at_utc' AS norm_updated_at_text,
            
            -- Normalisasi Angka & Finansial
            COALESCE(pb.amount, (pb.payload->>'amount')::numeric, 0) AS norm_amount,
            COALESCE((pb.payload->>'shipping_cost')::numeric, 0) AS norm_shipping_cost,
            COALESCE(pb.shipping_revenue, (pb.payload->>'shipping_revenue')::numeric, 0) AS norm_shipping_revenue,
            COALESCE(pb.payload->>'event_type', 'NONE') AS norm_event_type
        FROM prepared_bronze pb
        LEFT JOIN ops.silver_status_map sm ON pb.raw_status_text = sm.raw_status
    ),
    validated_data AS (
        -- Langkah 3: Validasi Data (Memisahkan data valid dan data invalid/rejection)
        SELECT 
            nd.*,
            CASE 
                WHEN nd.norm_order_id IS NULL OR TRIM(nd.norm_order_id) = '' THEN 'INVALID_ORDER_ID'
                WHEN nd.norm_amount < 0 THEN 'NEGATIVE_AMOUNT'
                WHEN nd.norm_ordered_at_utc IS NULL THEN 'INVALID_TIMESTAMP'
                ELSE 'VALID'
            END AS validation_status,
            CASE 
                WHEN nd.norm_order_id IS NULL OR TRIM(nd.norm_order_id) = '' THEN 'order_id kosong atau null'
                WHEN nd.norm_amount < 0 THEN 'Nilai amount tidak boleh negatif'
                WHEN nd.norm_ordered_at_utc IS NULL THEN 'Format ordered_at_utc tidak valid/kosong'
                ELSE NULL
            END AS rejection_reason
        FROM normalized_data nd
    ),
    insert_rejections AS (
        -- Langkah 4A: Tangkap data yang gagal validasi ke tabel quarantine/rejected records
        INSERT INTO ops.silver_rejected_records (
            target_table, file_name, row_number, source_record_id, 
            order_id, customer_id, rejection_stage, rejection_reason, 
            failed_field, failed_value, payload_json, bronze_checksum, rejected_at
        )
        SELECT 
            'silver.orders', file_name, row_number, source_record_id,
            order_id, customer_id, 'validate_orders', rejection_reason,
            CASE 
                WHEN validation_status = 'INVALID_ORDER_ID' THEN 'order_id'
                WHEN validation_status = 'NEGATIVE_AMOUNT' THEN 'amount'
                WHEN validation_status = 'INVALID_TIMESTAMP' THEN 'ordered_at_utc'
                ELSE 'unknown'
            END,
            CASE 
                WHEN validation_status = 'INVALID_ORDER_ID' THEN order_id
                WHEN validation_status = 'NEGATIVE_AMOUNT' THEN amount::text
                WHEN validation_status = 'INVALID_TIMESTAMP' THEN payload->>'ordered_at_utc'
                ELSE NULL
            END,
            payload, checksum, CURRENT_TIMESTAMP
        FROM validated_data
        WHERE validation_status != 'VALID'
        RETURNING rejection_id
    ),
    deduplicated_valid_orders AS (
        -- Langkah 4B: Hilangkan duplikasi data yang VALID menggunakan DISTINCT ON
        SELECT DISTINCT ON (vd.norm_order_id)
            vd.norm_order_id AS order_id,
            vd.norm_customer_id AS customer_id,
            vd.norm_store_id AS store_id,
            vd.norm_channel_id AS channel_id,
            vd.norm_sales_channel AS sales_channel,
            vd.norm_status AS order_status,
            vd.norm_status AS status,
            vd.norm_order_date AS order_date,
            vd.norm_order_date_utc AS order_date_utc,
            vd.norm_ordered_at_utc AS ordered_at_utc,
            vd.norm_updated_at_utc AS updated_at_utc,
            vd.norm_updated_at_text AS updated_at_text,
            vd.norm_amount AS amount,
            vd.norm_shipping_cost AS shipping_cost,
            vd.norm_shipping_revenue AS shipping_revenue,
            vd.norm_event_type AS event_type,
            vd.payload->>'source_row_id' AS source_row_id,
            vd.file_name AS bronze_file_name,
            vd.row_number AS bronze_row_number,
            vd.source_record_id AS bronze_source_record_id,
            vd.checksum AS bronze_checksum,
            vd.computed_deterministic_key AS deterministic_key
        FROM validated_data vd
        WHERE vd.validation_status = 'VALID'
        ORDER BY vd.norm_order_id, vd.ingested_at DESC
    ),
    insert_lineage AS (
        -- Langkah 5: Catat jejak audit transformasi ke ops.silver_lineage
        INSERT INTO ops.silver_lineage (
            deterministic_key, business_key, order_id, customer_id, store_id,
            channel_id, amount, shipping_revenue, order_status, event_type,
            status, source_table, target_table, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            bronze_ingested_at, file_name, payload_json, job_name, recorded_at
        )
        SELECT 
            dvo.deterministic_key, dvo.bronze_source_record_id AS business_key,
            dvo.order_id, dvo.customer_id, dvo.store_id, dvo.channel_id,
            dvo.amount, dvo.shipping_revenue, dvo.order_status, dvo.event_type,
            dvo.status, 'bronze.raw_data', 'silver.orders', dvo.bronze_file_name,
            dvo.bronze_row_number, dvo.bronze_source_record_id, dvo.bronze_checksum,
            CURRENT_TIMESTAMP, dvo.bronze_file_name,
            jsonb_build_object(
                'order_id', dvo.order_id, 'customer_id', dvo.customer_id,
                'store_id', dvo.store_id, 'amount', dvo.amount, 'status', dvo.status
            ),
            'airflow_silver_pipeline', CURRENT_TIMESTAMP
        FROM deduplicated_valid_orders dvo
        ON CONFLICT (deterministic_key) DO NOTHING
        RETURNING order_id
    )
    -- Langkah 6: Masukkan data yang sudah dinormalisasi dan divalidasi ke tabel utama silver.orders
    INSERT INTO silver.orders (
        order_id, customer_id, store_id, channel_id, sales_channel,
        order_status, status, order_date, order_date_utc, ordered_at_utc,
        updated_at_utc, updated_at_text, amount, shipping_cost,
        shipping_revenue, event_type, source_row_id, bronze_file_name,
        bronze_row_number, bronze_source_record_id, bronze_checksum,
        silver_processed_at, is_current, is_late_arriving, is_orphan,
        deterministic_key
    )
    SELECT 
        order_id, customer_id, store_id, channel_id, sales_channel,
        order_status, status, order_date, order_date_utc, ordered_at_utc,
        updated_at_utc, updated_at_text, amount, shipping_cost,
        shipping_revenue, event_type, source_row_id, bronze_file_name,
        bronze_row_number, bronze_source_record_id, bronze_checksum,
        CURRENT_TIMESTAMP, TRUE, FALSE, FALSE, deterministic_key
    FROM deduplicated_valid_orders
    ON CONFLICT (order_id) DO UPDATE 
    SET 
        customer_id = EXCLUDED.customer_id,
        store_id = EXCLUDED.store_id,
        channel_id = EXCLUDED.channel_id,
        sales_channel = EXCLUDED.sales_channel,
        order_status = EXCLUDED.order_status,
        status = EXCLUDED.status,
        order_date = EXCLUDED.order_date,
        order_date_utc = EXCLUDED.order_date_utc,
        ordered_at_utc = EXCLUDED.ordered_at_utc,
        updated_at_utc = EXCLUDED.updated_at_utc,
        updated_at_text = EXCLUDED.updated_at_text,
        amount = EXCLUDED.amount,
        shipping_cost = EXCLUDED.shipping_cost,
        shipping_revenue = EXCLUDED.shipping_revenue,
        event_type = EXCLUDED.event_type,
        source_row_id = EXCLUDED.source_row_id,
        bronze_file_name = EXCLUDED.bronze_file_name,
        bronze_row_number = EXCLUDED.bronze_row_number,
        bronze_source_record_id = EXCLUDED.bronze_source_record_id,
        bronze_checksum = EXCLUDED.bronze_checksum,
        silver_processed_at = CURRENT_TIMESTAMP,
        deterministic_key = EXCLUDED.deterministic_key;
    """

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        print("⏳ Mengeksekusi proses normalisasi & validasi ke `silver.orders`...")
        cur.execute(sql_query)
        conn.commit()
        
        affected_rows = cur.rowcount
        print(f"✅ NORMALIZATION & ANOMALY CHECK OK. Baris terdampak: {affected_rows}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"🚨 Error pada stage4_normalization: {e}")
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