import json
import pathlib
import traceback
from datetime import datetime
import psycopg2
from pyspark.sql import SparkSession


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
    conn = psycopg2.connect(
        host="postgres", 
        port=5432, 
        dbname="retail_analytics", 
        user="retail", 
        password="retail"
    )
    conn.autocommit = True
    return conn


def get_spark(app="SILVER_MEDALLION", **kwargs):
    return (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def stage11_quality_gate():
    print("\n=== 11. QUALITY GATE & REPORT (SILVER TO GOLD VALIDATION) ===")
    
    spark = get_spark("SILVER_REPORT")
    
    dq_query = """
    WITH qc AS (
        SELECT
            -- 1. Completeness: Memastikan kunci utama/Foreign key penting di silver.orders tidak NULL
            SUM(CASE WHEN order_id IS NULL OR customer_id IS NULL THEN 1 ELSE 0 END) AS null_cnt,

            -- 2. Uniqueness: Memastikan tidak ada duplikasi order_id di tabel orders
            COUNT(*) - COUNT(DISTINCT order_id) AS dup_cnt,

            -- 3. Business Rule / Domain Validation: Memastikan nilai amount tidak negatif
            SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS negative_cnt,

            -- 4. Future Date Validation: Memastikan tanggal transaksi (order_date) tidak di masa depan
            SUM(CASE WHEN order_date > CURRENT_DATE THEN 1 ELSE 0 END) AS future_date_cnt,

            -- 5. Domain Status Validation: Memastikan status order sesuai dengan mapping/domain yang valid
            SUM(
                CASE
                    WHEN LOWER(TRIM(COALESCE(status, order_status))) NOT IN ('paid','completed','cancelled','refunded','pending','failed','returned')
                    THEN 1
                    ELSE 0
                END
            ) AS invalid_status_cnt

        FROM silver.orders
    ),
    ri AS (
        -- 6. Referential Integrity (Orphan Check): Memastikan customer_id di silver.orders terdaftar di silver.customers
        SELECT
            COUNT(*) AS orphan_cnt
        FROM silver.orders o
        LEFT JOIN silver.customers c
            ON o.customer_id = c.customer_id
        WHERE o.customer_id IS NOT NULL 
          AND c.customer_id IS NULL
    ),
    freshness AS (
        -- 7. Freshness Check: Memastikan data di silver.orders sudah diproses/di-update dalam 1 hari terakhir
        SELECT
            CASE
                WHEN MAX(silver_processed_at) >= CURRENT_DATE - INTERVAL '1 DAY'
                THEN 0
                ELSE 1
            END AS stale_cnt
        FROM silver.orders
    ),
    recon AS (
        -- 8. Row Count Reconciliation: Membandingkan total baris dari raw data bronze ke tabel silver.orders
        SELECT
            ABS(
                (SELECT COUNT(*) FROM bronze.raw_data WHERE target_table = 'silver.orders' AND processing_status != 'FAILED')
                -
                (SELECT COUNT(*) FROM silver.orders)
            ) AS row_diff
    ),
    financial_recon AS (
        -- 9. Financial/Amount Reconciliation: Membandingkan total amount dari payload bronze dengan silver.orders
        SELECT
            CASE 
                WHEN ABS(
                    COALESCE((SELECT SUM(COALESCE(amount, (payload->>'amount')::numeric, 0)) FROM bronze.raw_data WHERE target_table = 'silver.orders'), 0) 
                    - 
                    COALESCE((SELECT SUM(amount) FROM silver.orders), 0)
                ) > 0.01 THEN 1
                ELSE 0
            END AS financial_diff_cnt
    )
    SELECT
        CASE
            WHEN null_cnt = 0
             AND dup_cnt = 0
             AND negative_cnt = 0
             AND future_date_cnt = 0
             AND invalid_status_cnt = 0
             AND orphan_cnt = 0
             AND stale_cnt = 0
             AND row_diff <= 1   -- DITOLERANSI: Menerima selisih maksimal 1 baris (hasil deduplikasi/karantina)
             AND financial_diff_cnt = 0
            THEN 'PASS'
            ELSE 'FAIL'
        END AS quality_gate,
        null_cnt,
        dup_cnt,
        negative_cnt,
        future_date_cnt,
        invalid_status_cnt,
        orphan_cnt,
        stale_cnt,
        row_diff,
        financial_diff_cnt
    FROM qc
    CROSS JOIN ri
    CROSS JOIN freshness
    CROSS JOIN recon
    CROSS JOIN financial_recon;
    """

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        print("⏳ Mengeksekusi validasi Quality Gate (9 Indikator Medallion)...")
        cur.execute(dq_query)
        result = cur.fetchone()
        
        # Ekstraksi hasil validasi
        gate_status = result[0]
        metrics = {
            "null_cnt": result[1],
            "dup_cnt": result[2],
            "negative_cnt": result[3],
            "future_date_cnt": result[4],
            "invalid_status_cnt": result[5],
            "orphan_cnt": result[6],
            "stale_cnt": result[7],
            "row_diff": result[8],
            "financial_diff_cnt": result[9]
        }

        print(f"📊 Hasil Evaluasi Quality Gate: {gate_status}")
        for k, v in metrics.items():
            print(f"   - {k}: {v}")

        # =========================================================================
        # 1. REKONSILIASI TABEL UTAMA UNTUK LAPORAN
        # =========================================================================
        cur.execute("""
            SELECT 'silver.orders', COUNT(*) FROM silver.orders 
            UNION ALL 
            SELECT 'silver.payment_events', COUNT(*) FROM silver.payment_events
        """)
        table_counts = cur.fetchall()
        print("\n📋 Rekonsiliasi Jumlah Baris Tabel:")
        for tbl, count_val in table_counts:
            print(f"   - {tbl}: {count_val} baris")

        # =========================================================================
        # 2. PENCATATAN LAPORAN KE DATABASE OPERASIONAL
        # =========================================================================
        log_insert_sql = """
        INSERT INTO ops.data_quality_log (check_name, table_name, status, details)
        VALUES (%s, %s, %s, %s);
        """
        cur.execute(log_insert_sql, (
            "SILVER_TO_GOLD_QUALITY_GATE",
            "silver.orders",
            gate_status,
            json.dumps(metrics)
        ))

        if gate_status == 'FAIL':
            for metric_name, count_val in metrics.items():
                # Jika metric row_diff, abaikan pencatatan anomaly jika masih dalam batas toleransi (<= 1)
                if metric_name == 'row_diff' and count_val <= 1:
                    continue

                if count_val > 0:
                    severity_level = 'critical' if metric_name in ['null_cnt', 'dup_cnt', 'financial_diff_cnt'] else 'warning'
                    
                    # Catat ke ops.silver_data_quality_events
                    event_insert_sql = """
                    INSERT INTO ops.silver_data_quality_events (
                        check_name, target_table, table_name, severity, 
                        issue_type, description, details_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """
                    cur.execute(event_insert_sql, (
                        "SILVER_TO_GOLD_QUALITY_GATE",
                        "silver.orders",
                        "silver.orders",
                        severity_level,
                        metric_name,
                        f"Quality gate check failed on metric: {metric_name} with count {count_val}",
                        json.dumps({"metric": metric_name, "failed_count": count_val})
                    ))

                    # Catat ke ops.silver_anomaly_log
                    anomaly_insert_sql = """
                    INSERT INTO ops.silver_anomaly_log (
                        check_name, target_table, issue_type, description, details_json, recorded_at
                    ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                    """
                    cur.execute(anomaly_insert_sql, (
                        "SILVER_TO_GOLD_QUALITY_GATE",
                        "silver.orders",
                        metric_name,
                        f"Anomaly detected in metric {metric_name} with count {count_val}",
                        json.dumps({"metric": metric_name, "failed_count": count_val})
                    ))

        conn.commit()

        # =========================================================================
        # 3. GENERASI FILE LAPORAN FISIK (TXT) DI DIRECTORY validation_report/
        # =========================================================================
        report_dir = pathlib.Path.cwd() / "validation_report"
        report_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        report_lines = [
            f"==================================================",
            f"SILVER TO GOLD QUALITY GATE REPORT - {ts}",
            f"==================================================",
            f"Overall Status: {gate_status}",
            f"\nDetailed Metrics:",
        ]
        for k, v in metrics.items():
            report_lines.append(f" - {k}: {v}")
            
        report_lines.append(f"\nDatabase Table Counts:")
        for tbl, count_val in table_counts:
            report_lines.append(f" - {tbl}: {count_val}")
            
        report_lines.append(f"\nEnd of Report.")
        
        report_path = report_dir / f"SILVER_FULL_FIXED_{ts}.txt"
        report_path.write_text("\n".join(report_lines), encoding="utf-8")
        print(f"📁 Laporan Teks Disimpan: {report_path}")

        # =========================================================================
        # 4. FITUR BLOKIR PIPELINE (RAISING EXCEPTION JIKA GAGAL)
        # =========================================================================
        if gate_status != 'PASS':
            raise ValueError(
                f"❌ QUALITY GATE FAILED! Data tidak memenuhi standar Medallion Architecture. "
                f"Laporan isu telah dicatat ke database dan file {report_path.name}. "
                f"Detail Metrik yang Gagal: {metrics}"
            )

        cur.execute("SELECT COUNT(*) FROM silver.orders;")
        cnt = cur.fetchone()[0]
        
        print(f"✅ QUALITY GATE PASSED & LAPORAN LENGKAP TERSIMPAN. Total baris valid: {cnt}")
        return f"quality_gate_ok_{cnt}"

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"🚨 Blokir Pipeline Aktif: {e}")
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
        try:
            spark.stop()
        except Exception:
            pass
        print("=" * 60 + "\n")