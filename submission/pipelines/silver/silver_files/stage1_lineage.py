"""Stage 1 Lineage: Process and load data from bronze raw_data to ops.silver_lineage using direct physical columns."""

from __future__ import annotations

import traceback
import psycopg2


def get_conn():
    """Establish connection to the database using Airflow connection or fallback defaults."""
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
        except Exception: 
            continue
            
    conn = psycopg2.connect(
        host="postgres", 
        port=5432, 
        dbname="retail_analytics", 
        user="retail", 
        password="retail"
    )
    conn.autocommit = True
    return conn


def stage1_lineage():
    """Extract raw data from bronze layer via physical columns, normalize, and populate ops.silver_lineage table."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        print(">> STAGE1: Ensure bronze + pull data dari bronze")
        
        # Inisialisasi fungsi immutable di schema bronze
        for sql in [
            "CREATE OR REPLACE FUNCTION bronze.immutable_timestamptz(t TEXT) RETURNS TIMESTAMPTZ AS $$ SELECT CASE WHEN t IS NULL OR t='' THEN NULL WHEN t ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN t::TIMESTAMPTZ ELSE NULL END $$ LANGUAGE sql IMMUTABLE;",
            "CREATE OR REPLACE FUNCTION bronze.immutable_date(t TEXT) RETURNS DATE AS $$ SELECT CASE WHEN t ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN t::DATE ELSE NULL END $$ LANGUAGE sql IMMUTABLE;",
            "CREATE OR REPLACE FUNCTION bronze.immutable_numeric(t TEXT) RETURNS NUMERIC AS $$ SELECT CASE WHEN t ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN t::NUMERIC ELSE NULL END $$ LANGUAGE sql IMMUTABLE;",
            "CREATE OR REPLACE FUNCTION bronze.immutable_int(t TEXT) RETURNS INT AS $$ SELECT CASE WHEN t ~ '^-?[0-9]+$' THEN t::INT ELSE NULL END $$ LANGUAGE sql IMMUTABLE;"
        ]:
            cur.execute(sql)
        conn.commit()

        # === STEP 1: AMBIL DATA DARI BRONZE ===
        cur.execute("SELECT COUNT(*) FROM bronze.raw_data;")
        bronze_cnt = cur.fetchone()[0]
        print(f">> STEP 1 - Ambil dari bronze: total_records={bronze_cnt}")

        if bronze_cnt == 0:
            print("🚨 BRONZE KOSONG! build_bronze_layer belum jalan")
            return

        cur.execute("TRUNCATE ops.silver_lineage RESTART IDENTITY CASCADE;")
        conn.commit()
        
        # === STEP 2: INSERT / UPSERT KE OPS.SILVER_LINEAGE ===
        cur.execute("""
            INSERT INTO ops.silver_lineage (
                deterministic_key,
                business_key,
                order_id,
                customer_id,
                product_id,
                payment_id,
                event_id,
                event_type,
                city_id,
                store_id,
                channel_id,
                address_id,
                order_item_id,
                promotion_id,
                campaign_id,
                refund_id,
                return_id,
                ticket_id,
                amount,
                shipping_revenue,
                quantity,
                order_status,
                payment_status,
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
                -- 1. Pembuatan Deterministic Key yang Unik dan Stabil (MD5 Hash)
                md5(
                    COALESCE(b.target_table, '') || '|' ||
                    COALESCE(b.source_record_id, b.bronze_id::text) || '|' ||
                    COALESCE(b.checksum, '') || '|' ||
                    COALESCE(b.row_number::text, '0')
                ) AS deterministic_key,

                -- 2. Business Key (Primary key bisnis dari sumber data)
                COALESCE(b.source_record_id, b.bronze_id::text) AS business_key,

                -- 3. Entitas Bisnis & Kunci Relasional
                b.order_id,
                b.customer_id,
                b.product_id,
                COALESCE(
                    b.payment_id,
                    b.event_id,
                    b.payload->>'payment_id',
                    b.payload #>> '{payload,payment_id}',
                    b.payload->>'id'
                ) AS payment_id,
                b.event_id,
                COALESCE(b.event_type, 'NONE') AS event_type,
                b.city_id,
                b.store_id,
                b.channel_id,
                b.address_id,
                b.order_item_id,
                b.promotion_id,
                b.campaign_id,
                b.refund_id,
                COALESCE(b.return_id, b.order_id) AS return_id,
                COALESCE(b.payload->>'ticket_id', b.payload #>> '{payload,ticket_id}') AS ticket_id,

                -- 4. Metrik Keuangan, Pendapatan Pengiriman, & Kuantitas
                COALESCE(b.amount, 0) AS amount,
                COALESCE(b.shipping_revenue, 0) AS shipping_revenue,
                COALESCE(b.quantity, b.stock_on_hand, 0) AS quantity,

                -- 5. Status Rincian & Normalisasi Teks
                b.status AS order_status,
                COALESCE(b.payload->>'payment_status', b.status, 'SUCCESS') AS payment_status,
                b.status,

                -- 6. Metadata Audit & Lineage
                'bronze.raw_data' AS source_table,
                b.target_table,
                b.file_name AS bronze_file_name,
                b.row_number AS bronze_row_number,
                b.source_record_id AS bronze_source_record_id,
                b.checksum AS bronze_checksum,
                b.ingested_at AS bronze_ingested_at,
                b.file_name,
                b.payload AS payload_json,
                'airflow_silver_pipeline' AS job_name,
                CURRENT_TIMESTAMP AS recorded_at

            FROM bronze.raw_data b
            WHERE b.processing_status = 'NEW'
            ON CONFLICT (deterministic_key) DO UPDATE
            SET 
                recorded_at = CURRENT_TIMESTAMP,
                status = EXCLUDED.status,
                payload_json = EXCLUDED.payload_json;
        """)
        conn.commit()
        
        # === STEP 3: LAPORAN REKAPITULASI ===
        cur.execute("SELECT target_table, COUNT(*) FROM ops.silver_lineage GROUP BY 1 ORDER BY 2 DESC;")
        print("=== FINAL LINEAGE BREAKDOWN ===") 
        for t, c in cur.fetchall():
            print(f" {c:>6} | {t}")

    except Exception as e:
        conn.rollback()
        print(f"FAIL stage1: {e}")
        traceback.print_exc()
        raise
    finally:
        cur.close()
        conn.close()