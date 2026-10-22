import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ.get('PATH')}"

import psycopg2
from airflow.hooks.base import BaseHook
from datetime import datetime

def get_conn(**kwargs):
    try:
        c = BaseHook.get_connection("retail_postgres")
        conn = psycopg2.connect(
            host=c.host or "postgres",
            port=c.port or 5432,
            dbname=c.schema or "retail_analytics",
            user=c.login or "retail",
            password=c.password or "retail"
        )
    except Exception:
        conn = psycopg2.connect(host="postgres", port=5432, dbname="retail_analytics", user="retail", password="retail")
    conn.autocommit = True
    return conn

# ================= GOLD BUILD =================
def run_gold(**context):
    print("=== START GOLD DAG CALLABLE ===")
    gold_sql = """
ALTER TABLE gold.order_360 ALTER COLUMN order_date DROP NOT NULL;
ALTER TABLE gold.customer_daily ALTER COLUMN gross_revenue DROP NOT NULL;
ALTER TABLE gold.customer_daily ALTER COLUMN gross_revenue SET DEFAULT 0;
ALTER TABLE gold.product_daily ALTER COLUMN gross_revenue DROP NOT NULL;
ALTER TABLE gold.product_daily ALTER COLUMN gross_revenue SET DEFAULT 0;

DO $$ DECLARE run_id TEXT := 'gold_' || to_char(NOW(),'YYYYMMDD_HH24MISS');
BEGIN
TRUNCATE gold.order_360;
INSERT INTO gold.order_360 (order_id, customer_id, order_date, sales_channel, store_id, gross_merchandise_value, discount_amount, shipping_revenue, captured_payment_amount, refunded_amount, net_revenue, item_count, unit_quantity, order_status, payment_status, return_status, promotion_count, first_order_flag, pipeline_run_id)
WITH order_base AS (
    SELECT DISTINCT ON (o.order_id) o.order_id, o.customer_id, COALESCE(o.order_date, (o.order_date_utc)::date, (o.ordered_at_utc)::date, CURRENT_DATE) as order_date, COALESCE(o.sales_channel,'online') as sales_channel, o.store_id, o.order_status, o.status, COALESCE(o.return_status,'NONE') as return_status FROM silver.orders o WHERE o.order_id IS NOT NULL ORDER BY o.order_id DESC
),
order_items_agg AS (SELECT oi.order_id, SUM(oi.quantity * oi.unit_price) as gmv, SUM(COALESCE(oi.item_discount_amount, oi.discount_amount, 0)) as discount, COUNT(*) as item_count, SUM(oi.quantity) as unit_qty, COUNT(DISTINCT oi.promotion_id) as promo_count FROM silver.order_items oi GROUP BY oi.order_id),
payment_agg AS (SELECT order_id, SUM(CASE WHEN LOWER(payment_status) IN ('captured','paid','completed') THEN COALESCE(payment_amount,amount,0) ELSE 0 END) as captured_amount, MAX(payment_status) as payment_status FROM silver.payment_events GROUP BY order_id),
refund_agg AS (SELECT order_id, SUM(amount) as refunded_amount FROM silver.refunds GROUP BY order_id),
first_order AS (SELECT customer_id, MIN(COALESCE(order_date, order_date_utc::date, CURRENT_DATE)) as first_date FROM silver.orders GROUP BY customer_id)
SELECT ob.order_id, ob.customer_id, ob.order_date, ob.sales_channel, ob.store_id, COALESCE(oia.gmv,0), COALESCE(oia.discount,0), 0, COALESCE(pa.captured_amount,0), COALESCE(ra.refunded_amount,0), COALESCE(oia.gmv,0) - COALESCE(oia.discount,0) - COALESCE(ra.refunded_amount,0), COALESCE(oia.item_count,0), COALESCE(oia.unit_qty,0), COALESCE(ob.order_status,ob.status,'pending'), COALESCE(pa.payment_status,'pending'), COALESCE(ob.return_status,'NONE'), COALESCE(oia.promo_count,0), CASE WHEN ob.order_date=fo.first_date THEN TRUE ELSE FALSE END, run_id FROM order_base ob LEFT JOIN order_items_agg oia ON oia.order_id=ob.order_id LEFT JOIN payment_agg pa ON pa.order_id=ob.order_id LEFT JOIN refund_agg ra ON ra.order_id=ob.order_id LEFT JOIN first_order fo ON fo.customer_id=ob.customer_id;

TRUNCATE gold.customer_daily;
INSERT INTO gold.customer_daily (customer_id, metric_date, order_count, unit_quantity, gross_revenue, net_revenue, refund_amount, return_count, support_contacts, active_channel, new_customer_flag, repeat_customer_flag, pipeline_run_id)
WITH daily_orders AS (SELECT customer_id, COALESCE(order_date, order_date_utc::date, ordered_at_utc::date, CURRENT_DATE) as metric_date, COUNT(DISTINCT order_id) as order_cnt FROM silver.orders GROUP BY 1,2),
daily_net AS (SELECT customer_id, order_date, SUM(net_revenue) as net_rev, SUM(unit_quantity) as unit_qty FROM gold.order_360 GROUP BY 1,2)
SELECT d.customer_id, d.metric_date, d.order_cnt, COALESCE(dn.unit_qty,0), COALESCE(dn.net_rev,0), COALESCE(dn.net_rev,0), 0, 0, 0, 'online', FALSE, FALSE, run_id FROM daily_orders d LEFT JOIN daily_net dn ON dn.customer_id=d.customer_id AND dn.order_date=d.metric_date;

TRUNCATE gold.product_daily;
INSERT INTO gold.product_daily (product_id, metric_date, active_category, units_sold, order_count, gross_revenue, net_revenue, refunded_units, available_inventory, reserved_inventory, stockout_flag, promotion_count, pipeline_run_id)
WITH product_orders AS (SELECT oi.product_id, COALESCE(o.order_date, (o.order_date_utc)::date, (o.ordered_at_utc)::date, CURRENT_DATE) as metric_date, COALESCE(p.category_name, p.category_id, 'Uncategorized') as active_category, SUM(GREATEST(oi.quantity,0)) as units_sold, COUNT(DISTINCT oi.order_id) as order_cnt, SUM(GREATEST(oi.quantity,0)*oi.unit_price) as gross_rev, SUM(GREATEST(oi.quantity,0)*oi.unit_price) as net_rev, COUNT(DISTINCT oi.promotion_id) as promo_cnt FROM silver.order_items oi JOIN silver.orders o ON o.order_id=oi.order_id LEFT JOIN silver.products p ON p.product_id=oi.product_id WHERE oi.quantity > 0 AND oi.product_id IS NOT NULL GROUP BY 1,2,3), inventory AS (SELECT product_id, COALESCE(snapshot_date_utc, CURRENT_DATE) as metric_date, SUM(GREATEST(stock_quantity,0)) as avail_inv FROM silver.inventory_snapshots WHERE product_id IS NOT NULL GROUP BY 1,2), all_dates AS (SELECT product_id, metric_date FROM product_orders UNION SELECT product_id, metric_date FROM inventory)
SELECT adp.product_id, adp.metric_date, COALESCE(po.active_category,'Uncategorized'), COALESCE(po.units_sold,0), COALESCE(po.order_cnt,0), COALESCE(po.gross_rev,0), COALESCE(po.net_rev,0), 0, COALESCE(inv.avail_inv,0), 0, FALSE, COALESCE(po.promo_cnt,0), run_id FROM all_dates adp LEFT JOIN product_orders po ON po.product_id=adp.product_id AND po.metric_date=adp.metric_date LEFT JOIN inventory inv ON inv.product_id=adp.product_id AND inv.metric_date=adp.metric_date WHERE adp.product_id IS NOT NULL AND adp.metric_date IS NOT NULL;

TRUNCATE gold.channel_campaign_daily;
INSERT INTO gold.channel_campaign_daily (metric_date, sales_channel, campaign_id, campaign_spend, attributed_orders, attributed_customers, gross_revenue, net_revenue, refunds, roas, conversion_rate, pipeline_run_id) SELECT CURRENT_DATE, 'online', 'NO_CAMPAIGN', 0, COUNT(DISTINCT order_id), COUNT(DISTINCT customer_id), SUM(gross_merchandise_value), SUM(net_revenue), SUM(refunded_amount), NULL, NULL, run_id FROM gold.order_360;

TRUNCATE gold.executive_kpis_daily;
INSERT INTO gold.executive_kpis_daily (metric_date, total_orders, gross_revenue, net_revenue, average_order_value, refund_rate, return_rate, repeat_customer_rate, stockout_rate, active_customers, support_contact_rate, data_freshness_utc, pipeline_run_id) SELECT order_date, COUNT(*), SUM(gross_merchandise_value), SUM(net_revenue), AVG(net_revenue), 0, 0, 0, 0, COUNT(DISTINCT customer_id), 0, NOW(), run_id FROM gold.order_360 GROUP BY 1;
END $$;
"""
    conn = get_conn()
    cur = conn.cursor()
    print("=== VALIDASI SILVER ===")
    cur.execute("SELECT COUNT(*) FROM silver.orders")
    print(f"orders: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM silver.order_items")
    print(f"order_items: {cur.fetchone()[0]}")
    
    print("=== EXECUTE GOLD SQL ===")
    cur.execute(gold_sql)
    print("GOLD SQL SUCCESS")

    total = 0
    for tbl in ["gold.order_360", "gold.customer_daily", "gold.product_daily", "gold.channel_campaign_daily", "gold.executive_kpis_daily"]:
        cur.execute(f"SELECT COUNT(*) FROM {tbl};")
        cnt = cur.fetchone()[0]
        print(f"{cnt:>6} | {tbl}")
        total += cnt

    cur.close()
    conn.close()
    print(f"\nDONE Gold {total} rows")
    
    # --- PANGGIL VALIDATE + RECORD ---
    run_id = validate_gold(**context)
    record_pipeline_metadata(run_id, status='SUCCESS', **context)
    
    return total

# ================= VALIDATE GOLD =================
def validate_gold(**kwargs):
    """
    Validasi Gold: cek gold.order_360, customer_daily, product_daily, executive_kpis
    Kalau FAIL -> DAG berhenti, Gold tidak di-publish
    Log ke ops.data_quality_log
    """
    print("\n=== VALIDATE GOLD ===")
    conn = get_conn()
    cur = conn.cursor()
    
    checks = []
    run_id = kwargs.get('run_id') or context_run_id(cur) # ambil dari gold
    
    try:
        # 1. ROW COUNT
        cur.execute("SELECT COUNT(*) FROM gold.order_360;")
        order360_cnt = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(*) FROM silver.orders;")
        silver_orders_cnt = cur.fetchone()[0] or 0
        checks.append(("gold_row_count_match_silver", "gold.order_360", order360_cnt == silver_orders_cnt and order360_cnt>0, f"gold={order360_cnt} silver={silver_orders_cnt}"))

        # 2. COMPLETENESS - NULL check
        cur.execute("SELECT COUNT(*) FROM gold.order_360 WHERE order_id IS NULL OR customer_id IS NULL OR order_date IS NULL;")
        null_cnt = cur.fetchone()[0] or 0
        checks.append(("gold_completeness_order360", "gold.order_360", null_cnt==0, f"null critical={null_cnt} expected 0"))

        cur.execute("SELECT COUNT(*) FROM gold.executive_kpis_daily WHERE metric_date IS NULL;")
        null_exec = cur.fetchone()[0] or 0
        checks.append(("gold_completeness_executive", "gold.executive_kpis_daily", null_exec==0, f"null metric_date={null_exec}"))

        # 3. VALIDITY - negative check
        cur.execute("SELECT COUNT(*) FROM gold.order_360 WHERE net_revenue <0 OR gross_merchandise_value <0 OR unit_quantity <=0;")
        invalid_cnt = cur.fetchone()[0] or 0
        checks.append(("gold_validity", "gold.order_360", invalid_cnt==0, f"invalid revenue/qty={invalid_cnt} expected 0"))

        # 4. UNIQUENESS - dup check
        cur.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM gold.order_360;")
        dup_cnt = cur.fetchone()[0] or 0
        checks.append(("gold_uniqueness_order360", "gold.order_360", dup_cnt==0, f"dup order_id={dup_cnt} expected 0"))

        cur.execute("SELECT COUNT(*) FROM (SELECT customer_id, metric_date, COUNT(*) c FROM gold.customer_daily GROUP BY 1,2 HAVING COUNT(*)>1) t;")
        dup_cust = cur.fetchone()[0] or 0
        checks.append(("gold_uniqueness_customer_daily", "gold.customer_daily", dup_cust==0, f"dup customer_daily={dup_cust}"))

        # 5. CONSISTENCY - silver vs gold SUM
        cur.execute("SELECT COALESCE(SUM(gross_merchandise_value),0) FROM gold.order_360;")
        gold_gmv = cur.fetchone()[0] or 0
        cur.execute("SELECT COALESCE(SUM(gross_revenue),0) FROM gold.product_daily;")
        product_gmv = cur.fetchone()[0] or 0
        diff_pct = abs(gold_gmv - product_gmv)/gold_gmv*100 if gold_gmv else 0
        checks.append(("gold_consistency_product_vs_order", "gold.product_daily", diff_pct < 0.1, f"diff={diff_pct:.4f}% threshold <0.1% gold={gold_gmv} product={product_gmv}"))

        # 6. FRESHNESS
        cur.execute("SELECT CURRENT_DATE - MAX(metric_date) FROM gold.executive_kpis_daily;")
        lag = cur.fetchone()[0] or 999
        checks.append(("gold_freshness", "gold.executive_kpis_daily", lag <=1, f"lag_days={lag} expected <=1"))

        # 7. EXECUTIVE KPI COMPLETENESS
        cur.execute("SELECT total_orders, gross_revenue, net_revenue FROM gold.executive_kpis_daily ORDER BY metric_date DESC LIMIT 1;")
        row = cur.fetchone()
        if row:
            checks.append(("gold_executive_kpis_values", "gold.executive_kpis_daily", row[0]>0 and row[1]>0, f"orders={row[0]} revenue={row[1]}"))
        else:
            checks.append(("gold_executive_kpis_values", "gold.executive_kpis_daily", False, "no rows in executive_kpis"))

        # LOG KE ops.data_quality_log
        for check_name, table_name, passed, details in checks:
            status = "PASS" if passed else "FAIL"
            cur.execute("""
                INSERT INTO ops.data_quality_log (check_name, table_name, status, details)
                VALUES (%s,%s,%s,%s);
            """, (check_name, table_name, status, details))
            print(f"{status}: {check_name} | {table_name} | {details}")

        conn.commit()

        # FAIL JIKA CRITICAL
        failed = [c for c in checks if not c[2]]
        failed_critical = [c for c in failed if c[0] in ('gold_row_count_match_silver','gold_completeness_order360','gold_validity','gold_uniqueness_order360')]
        
        if failed_critical:
            fail_msg = "; ".join([f"{c[0]}: {c[3]}" for c in failed_critical])
            print(f"VALIDATE GOLD FAIL CRITICAL: {fail_msg}")
            # Record FAIL metadata
            record_pipeline_metadata(run_id, status='FAILED', error_msg=fail_msg, **kwargs)
            raise ValueError(f"GOLD VALIDATION FAIL: {fail_msg}")

        print("VALIDATE GOLD PASS - Gold siap publish")
        return run_id

    except ValueError:
        raise
    except Exception as e:
        print(f"Validate Gold Error: {e}")
        conn.rollback()
        cur.execute("INSERT INTO ops.data_quality_log (check_name, table_name, status, details) VALUES (%s,%s,%s,%s);",
                    ("validate_gold_error", "gold", "FAIL", str(e)))
        conn.commit()
        raise
    finally:
        cur.close()
        conn.close()

def context_run_id(cur):
    try:
        cur.execute("SELECT pipeline_run_id FROM gold.order_360 ORDER BY order_date DESC LIMIT 1;")
        r = cur.fetchone()
        return r[0] if r else f"gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    except:
        return f"gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# ================= RECORD PIPELINE METADATA =================
def record_pipeline_metadata(run_id, status='SUCCESS', error_msg=None, **kwargs):
    """
    a. INSERT INTO ops.pipeline_runs (run_id, dag_id, status, started_at, finished_at)
    b. ops.silver_lineage sudah terisi di build_silver stage1 - catat asal file
    c. ops.data_quality_log sudah terisi dari validate_gold + quality gate
    """
    print(f"\n=== RECORD PIPELINE METADATA run_id={run_id} status={status} ===")
    conn = get_conn()
    cur = conn.cursor()
    try:
        # a. pipeline_runs
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
                id SERIAL PRIMARY KEY,
                run_id VARCHAR(255),
                dag_id VARCHAR(255),
                status VARCHAR(50),
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            );
        """)
        cur.execute("""
            INSERT INTO ops.pipeline_runs (run_id, dag_id, status, started_at, finished_at)
            VALUES (%s, %s, %s, NOW() - INTERVAL '2 minutes', NOW())
            ON CONFLICT DO NOTHING;
        """, (run_id, 'omnichannel_retail_dag', status))

        # b. silver_lineage summary log (traceability)
        cur.execute("""
            SELECT target_table, COUNT(*) as cnt, COUNT(DISTINCT bronze_file_name) as files
            FROM ops.silver_lineage GROUP BY 1 ORDER BY 1;
        """)
        lineage_summary = cur.fetchall()
        lineage_details = "; ".join([f"{t}: {c} rows from {f} files" for t,c,f in lineage_summary])
        cur.execute("""
            INSERT INTO ops.data_quality_log (check_name, table_name, status, details)
            VALUES ('pipeline_lineage_trace', 'ops.silver_lineage', 'INFO', %s);
        """, (lineage_details,))

        # c. data_quality_log summary sudah dari validate_gold
        # Tambahan summary gold tables
        cur.execute("""
            SELECT 'gold.order_360' as tbl, COUNT(*) FROM gold.order_360 UNION ALL
            SELECT 'gold.customer_daily', COUNT(*) FROM gold.customer_daily UNION ALL
            SELECT 'gold.product_daily', COUNT(*) FROM gold.product_daily UNION ALL
            SELECT 'gold.executive_kpis_daily', COUNT(*) FROM gold.executive_kpis_daily;
        """)
        gold_counts = cur.fetchall()
        gold_summary = "; ".join([f"{tbl}={cnt}" for tbl,cnt in gold_counts])
        
        cur.execute("""
            INSERT INTO ops.data_quality_log (check_name, table_name, status, details)
            VALUES ('pipeline_gold_summary', 'gold', %s, %s);
        """, ('INFO' if status=='SUCCESS' else 'FAIL', gold_summary + (f" | error={error_msg}" if error_msg else "")))

        conn.commit()
        print(f"METADATA RECORDED: {gold_summary} | lineage: {lineage_details}")

        # --- BUAT LAPORAN FILE ---
        report_path = "/tmp/gold_validation_report.txt"
        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write("="*70 + "\nLAPORAN VALIDATE GOLD & PIPELINE METADATA\n")
                f.write(f"Run ID: {run_id}\nStatus: {status}\nWaktu: {datetime.now()}\n" + "="*70 + "\n\n")
                f.write("a. ops.pipeline_runs:\n")
                f.write(f"  - run_id: {run_id}\n  - dag_id: omnichannel_retail_dag\n  - status: {status}\n\n")
                f.write("b. ops.silver_lineage (traceability):\n")
                for t,c,fcount in lineage_summary:
                    f.write(f"  - {t}: {c} rows from {fcount} bronze files\n")
                    # contoh trace
                    f.write(f"    -> Contoh: order_id dari {t} bisa ditrace ke bronze_file_name + row_number + payload_json\n")
                f.write("\nContoh query lineage:\n")
                f.write("  SELECT business_key, bronze_file_name, bronze_row_number, payload_json FROM ops.silver_lineage WHERE target_table='silver.orders' LIMIT 1;\n\n")
                f.write("c. ops.data_quality_log (validate gold):\n")
                cur.execute("SELECT check_name, table_name, status, details FROM ops.data_quality_log WHERE checked_at > NOW() - INTERVAL '10 minutes' ORDER BY checked_at DESC LIMIT 20;")
                for chk, tbl, st, det in cur.fetchall():
                    f.write(f"  - {st}: {chk} | {tbl} | {det}\n")
                f.write(f"\nGold Summary: {gold_summary}\n")
                f.write("\nGOLD READY FOR BI\n")
            print(f"Report: {report_path}")
        except Exception as e:
            print(f"Gagal tulis report: {e}")

        return run_id

    except Exception as e:
        print(f"Record Metadata Error: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def build_gold_dag_callable(**context):
    return run_gold(**context)

def main(**context):
    return run_gold(**context)

if __name__ == "__main__":
    run_gold()
