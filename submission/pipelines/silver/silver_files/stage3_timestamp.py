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

def stage3_timestamp():
    print("\n=== 2. TIMESTAMP & TIMEZONE STANDARDIZATION (SILVER.ORDERS) ===")

    sql_query = """
        WITH prepared_bronze AS (
            -- Langkah 1: Saring data mentah khusus untuk silver.orders dari bronze
            SELECT 
                b.order_id,
                b.customer_id,
                b.store_id,
                b.channel_id,
                b.sales_channel,
                b.status AS order_status,
                b.status,
                b.amount,
                b.shipping_revenue,
                b.event_type,
                b.payload,
                b.source_record_id,
                b.file_name,
                b.row_number,
                b.checksum,
                b.ingested_at,
                b.target_table,
                md5(
                    COALESCE(b.target_table, '') || '|' ||
                    COALESCE(b.source_record_id, b.bronze_id::text) || '|' ||
                    COALESCE(b.checksum, '') || '|' ||
                    COALESCE(b.row_number::text, '0')
                ) AS computed_deterministic_key
            FROM bronze.raw_data b
            WHERE b.processing_status = 'NEW'
              AND b.target_table = 'silver.orders'
              AND b.order_id IS NOT NULL
        ),
        deduplicated_orders AS (
            -- Langkah 2: Hilangkan duplikasi menggunakan DISTINCT ON berdasarkan order_id
            SELECT DISTINCT ON (pb.order_id)
                pb.order_id,
                pb.customer_id,
                pb.store_id,
                pb.channel_id,
                COALESCE(pb.sales_channel, pb.payload->>'sales_channel', 'online') AS sales_channel,
                pb.order_status,
                pb.status,
                NULLIF(pb.payload->>'order_date', '')::DATE AS order_date,
                NULLIF(pb.payload->>'order_date_utc', '')::TIMESTAMPTZ AS order_date_utc,
                NULLIF(pb.payload->>'ordered_at_utc', '')::TIMESTAMPTZ AS ordered_at_utc,
                NULLIF(pb.payload->>'updated_at_utc', '')::TIMESTAMPTZ AS updated_at_utc,
                pb.payload->>'updated_at_utc' AS updated_at_text,
                COALESCE(pb.amount, (pb.payload->>'amount')::numeric, 0) AS amount,
                COALESCE((pb.payload->>'shipping_cost')::numeric, 0) AS shipping_cost,
                COALESCE(pb.shipping_revenue, (pb.payload->>'shipping_revenue')::numeric, 0) AS shipping_revenue,
                COALESCE(pb.event_type, pb.payload->>'event_type', 'NONE') AS event_type,
                pb.payload->>'source_row_id' AS source_row_id,
                pb.file_name AS bronze_file_name,
                pb.row_number AS bronze_row_number,
                pb.source_record_id AS bronze_source_record_id,
                pb.checksum AS bronze_checksum,
                pb.computed_deterministic_key AS deterministic_key
            FROM prepared_bronze pb
            ORDER BY pb.order_id, pb.ingested_at DESC
        ),
        insert_lineage AS (
            -- Langkah 3: Catat jejak transformasi ke tabel ops.silver_lineage
            INSERT INTO ops.silver_lineage (
                deterministic_key,
                business_key,
                order_id,
                customer_id,
                store_id,
                channel_id,
                amount,
                shipping_revenue,
                order_status,
                event_type,
                status,
                source_table,
                target_table,
                bronze_file_name,
                bronze_row_number,
                bronze_source_record_id,
                bronze_checksum,
                bronze_ingested_at,
                file_name,
                payload_json,
                job_name,
                recorded_at
            )
            SELECT 
                do_item.deterministic_key,
                do_item.bronze_source_record_id AS business_key,
                do_item.order_id,
                do_item.customer_id,
                do_item.store_id,
                do_item.channel_id,
                do_item.amount,
                do_item.shipping_revenue,
                do_item.order_status,
                do_item.event_type,
                do_item.status,
                'bronze.raw_data',
                'silver.orders',
                do_item.bronze_file_name,
                do_item.bronze_row_number,
                do_item.bronze_source_record_id,
                do_item.bronze_checksum,
                CURRENT_TIMESTAMP,
                do_item.bronze_file_name,
                jsonb_build_object(
                    'order_id', do_item.order_id,
                    'customer_id', do_item.customer_id,
                    'store_id', do_item.store_id,
                    'channel_id', do_item.channel_id,
                    'amount', do_item.amount,
                    'status', do_item.status
                ),
                'airflow_silver_pipeline',
                CURRENT_TIMESTAMP
            FROM deduplicated_orders do_item
            ON CONFLICT (deterministic_key) DO NOTHING
            RETURNING order_id
        )
        -- Langkah 4: Masukkan atau perbarui data ke tabel utama silver.orders
        INSERT INTO silver.orders (
            order_id,
            customer_id,
            store_id,
            channel_id,
            sales_channel,
            order_status,
            status,
            order_date,
            order_date_utc,
            ordered_at_utc,
            updated_at_utc,
            updated_at_text,
            amount,
            shipping_cost,
            shipping_revenue,
            event_type,
            source_row_id,
            bronze_file_name,
            bronze_row_number,
            bronze_source_record_id,
            bronze_checksum,
            silver_processed_at,
            is_current,
            is_late_arriving,
            is_orphan,
            deterministic_key
        )
        SELECT 
            order_id,
            customer_id,
            store_id,
            channel_id,
            sales_channel,
            order_status,
            status,
            order_date,
            order_date_utc,
            ordered_at_utc,
            updated_at_utc,
            updated_at_text,
            amount,
            shipping_cost,
            shipping_revenue,
            event_type,
            source_row_id,
            bronze_file_name,
            bronze_row_number,
            bronze_source_record_id,
            bronze_checksum,
            CURRENT_TIMESTAMP,
            TRUE,
            FALSE,
            FALSE,
            deterministic_key
        FROM deduplicated_orders
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
        
        print("⏳ Mengeksekusi standarisasi timestamp dan upsert ke silver.orders...")
        cur.execute(sql_query)
        conn.commit()
        
        affected_rows = cur.rowcount
        print(f"✅ Berhasil memproses data `silver.orders`. Baris terdampak: {affected_rows}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"🚨 Error pada stage3_timestamp (silver.orders): {e}")
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