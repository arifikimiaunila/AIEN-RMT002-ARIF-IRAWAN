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

def stage7_identity():
    print("\n=== 7. IDENTITY RESOLUTION & FACT RETURN VIEW (SILVER.PAYMENT_EVENTS) ===")
    
    sql_query = """
    -- =========================================================================
    -- ETL PIPELINE: BRONZE TO SILVER (IDENTITY RESOLUTION WITH CTE)
    -- =========================================================================
    WITH prepared_bronze_events AS (
        -- Langkah 1: Saring data mentah event dari bronze untuk resolusi identitas
        SELECT 
            b.bronze_id,
            b.payment_id,
            b.order_id,
            b.event_id,
            b.event_type,
            b.payload,
            b.source_record_id,
            b.file_name,
            b.row_number,
            b.checksum,
            b.ingested_at,
            b.target_table,
            COALESCE(b.payment_id, b.payload->>'payment_id', b.order_id, b.payload->>'order_id') AS business_key,
            COALESCE(b.payload->>'payment_status', b.payload->>'status', 'pending') AS raw_status,
            -- Ekstraksi timestamp terjadinya event secara aman
            COALESCE(
                NULLIF(b.payload->>'occurred_at_utc', '')::TIMESTAMPTZ,
                NULLIF(b.payload->>'payment_event_time', '')::TIMESTAMPTZ,
                b.ingested_at
            ) AS event_timestamp
        FROM bronze.raw_data b
        WHERE b.processing_status = 'NEW'
          AND b.target_table = 'silver.payment_events'
    ),
    ranked_identities AS (
        -- Langkah 2: Lakukan perankingan / Window Function untuk identifikasi competing IDs 
        SELECT 
            pe.*,
            ROW_NUMBER() OVER (
                PARTITION BY pe.business_key 
                ORDER BY pe.event_timestamp DESC, pe.ingested_at DESC
            ) as identity_rank,
            -- Kumpulkan semua competing event_id ke dalam array untuk keperluan audit
            array_agg(pe.event_id) OVER (PARTITION BY pe.business_key) as all_competing_ids
        FROM prepared_bronze_events pe
    ),
    resolved_identities AS (
        -- Langkah 3: Tentukan pemenang (Winning ID) dan aturan resolusi identitas
        SELECT 
            business_key AS event_business_key,
            event_id,
            event_type,
            order_id,
            payment_id,
            -- Resolved ID: Menggunakan ID dari baris dengan peringkat tertinggi (terbaru/valid)
            COALESCE(event_id, payment_id, business_key) AS resolved_id,
            all_competing_ids AS competing_event_ids,
            'LATEST_TIMESTAMP_PRIORITY' AS resolution_rule,
            raw_status AS winning_status,
            payload AS payload_json,
            target_table
        FROM ranked_identities
        WHERE identity_rank = 1
    )
    -- Langkah 4: Masukkan hasil Identity Resolution ke tabel operasional ops.silver_event_identity
    INSERT INTO ops.silver_event_identity (
        target_table,
        event_business_key,
        event_id,
        event_type,
        order_id,
        payment_id,
        resolved_id,
        competing_event_ids,
        resolution_rule,
        winning_status,
        payload_json,
        resolved_at
    )
    SELECT 
        target_table,
        event_business_key,
        event_id,
        event_type,
        order_id,
        payment_id,
        resolved_id,
        competing_event_ids,
        resolution_rule,
        winning_status,
        payload_json,
        CURRENT_TIMESTAMP
    FROM resolved_identities
    ON CONFLICT DO NOTHING;
    """

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        print("⏳ Mengeksekusi proses identity resolution ke `ops.silver_event_identity`...")
        cur.execute(sql_query)
        conn.commit()
        
        affected_rows = cur.rowcount
        print(f"✅ IDENTITY RESOLUTION OK. Baris terdampak: {affected_rows}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"🚨 Error pada stage7_identity: {e}")
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