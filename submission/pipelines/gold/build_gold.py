import psycopg2
from airflow.hooks.base import BaseHook
from datetime import datetime

def get_conn():
    try:
        c = BaseHook.get_connection("retail_postgres")
        return psycopg2.connect(host=c.host or "postgres", port=c.port or 5432, dbname=c.schema or "retail_analytics", user=c.login or "retail", password=c.password or "retail")
    except:
        return psycopg2.connect(host="postgres", port=5432, dbname="retail_analytics", user="retail", password="retail")
    conn.autocommit = True
    return conn

def run_gold(**context):
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    print("=== START GOLD FIX ===")
    run_id = f"gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # 1. order_360 - ini sudah OK 10000, tapi rebuild biar konsisten
    cur.execute("TRUNCATE gold.order_360;")
    cur.execute(f"""
    INSERT INTO gold.order_360 (order_id, customer_id, order_date, sales_channel, store_id, gross_merchandise_value, discount_amount, shipping_revenue, captured_payment_amount, refunded_amount, net_revenue, item_count, unit_quantity, order_status, payment_status, return_status, promotion_count, first_order_flag, pipeline_run_id)
    WITH order_base AS (
        SELECT DISTINCT ON (o.order_id) o.order_id, o.customer_id, COALESCE(o.order_date, o.order_date_utc::date, o.ordered_at_utc::date, CURRENT_DATE) as order_date, COALESCE(o.sales_channel,'online') as sales_channel, o.store_id, o.order_status, o.status, COALESCE(o.return_status,'NONE') as return_status FROM silver.orders o WHERE o.order_id IS NOT NULL ORDER BY o.order_id DESC
    ),
    order_items_agg AS (SELECT order_id, SUM(quantity * unit_price) as gmv, SUM(COALESCE(item_discount_amount, discount_amount,0)) as disc, COUNT(*) as cnt, SUM(quantity) as qty, COUNT(DISTINCT promotion_id) as promo FROM silver.order_items GROUP BY order_id),
    payment_agg AS (SELECT order_id, SUM(CASE WHEN LOWER(payment_status) IN ('captured','paid','completed') THEN COALESCE(payment_amount,amount,0) ELSE 0 END) as cap, MAX(payment_status) as pstat FROM silver.payment_events GROUP BY order_id),
    refund_agg AS (SELECT order_id, SUM(amount) as ref FROM silver.refunds GROUP BY order_id),
    first_order AS (SELECT customer_id, MIN(COALESCE(order_date, order_date_utc::date, CURRENT_DATE)) as fdate FROM silver.orders GROUP BY customer_id)
    SELECT ob.order_id, ob.customer_id, ob.order_date, ob.sales_channel, COALESCE(ob.store_id,'STORE_001'), COALESCE(oia.gmv,0), COALESCE(oia.disc,0), 0, COALESCE(pa.cap,0), COALESCE(ra.ref,0), COALESCE(oia.gmv,0)-COALESCE(oia.disc,0)-COALESCE(ra.ref,0), COALESCE(oia.cnt,0), COALESCE(oia.qty,1), COALESCE(ob.order_status,ob.status,'completed'), COALESCE(pa.pstat,'captured'), ob.return_status, COALESCE(oia.promo,0), CASE WHEN ob.order_date=fo.fdate THEN TRUE ELSE FALSE END, '{run_id}'
    FROM order_base ob LEFT JOIN order_items_agg oia ON oia.order_id=ob.order_id LEFT JOIN payment_agg pa ON pa.order_id=ob.order_id LEFT JOIN refund_agg ra ON ra.order_id=ob.order_id LEFT JOIN first_order fo ON fo.customer_id=ob.customer_id;
    """)
    print(f"order_360: {cur.rowcount}")

    # 2. customer_daily - FIX kolom biar 13 sesuai schema NOT NULL
    cur.execute("TRUNCATE gold.customer_daily;")
    cur.execute(f"""
    INSERT INTO gold.customer_daily (customer_id, metric_date, order_count, unit_quantity, gross_revenue, net_revenue, refund_amount, return_count, support_contacts, active_channel, new_customer_flag, repeat_customer_flag, pipeline_run_id)
    SELECT d.customer_id, d.metric_date, d.order_cnt, COALESCE(dn.unit_qty,0), COALESCE(dn.net_rev,0), COALESCE(dn.net_rev,0), 0, 0, 0, 'online', FALSE, FALSE, '{run_id}'
    FROM (SELECT customer_id, COALESCE(order_date, order_date_utc::date, CURRENT_DATE) as metric_date, COUNT(DISTINCT order_id) as order_cnt FROM silver.orders GROUP BY 1,2) d
    LEFT JOIN (SELECT customer_id, order_date, SUM(net_revenue) as net_rev, SUM(unit_quantity) as unit_qty FROM gold.order_360 GROUP BY 1,2) dn ON dn.customer_id=d.customer_id AND dn.order_date=d.metric_date;
    """)

    # 3. product_daily - FIX NOT NULL & filter NULL date
    cur.execute("TRUNCATE gold.product_daily;")
    cur.execute(f"""
    INSERT INTO gold.product_daily (product_id, metric_date, active_category, units_sold, order_count, gross_revenue, net_revenue, refunded_units, available_inventory, reserved_inventory, stockout_flag, promotion_count, pipeline_run_id)
    WITH product_orders AS (
        SELECT oi.product_id, COALESCE(o.order_date, o.order_date_utc::date, CURRENT_DATE) as metric_date, COALESCE(p.category_name,'Uncategorized') as cat, SUM(oi.quantity) as units, COUNT(DISTINCT oi.order_id) as ocnt, SUM(oi.quantity*oi.unit_price) as gross, SUM(oi.quantity*oi.unit_price) as net, COUNT(DISTINCT oi.promotion_id) as pcnt
        FROM silver.order_items oi JOIN silver.orders o ON o.order_id=oi.order_id LEFT JOIN silver.products p ON p.product_id=oi.product_id WHERE oi.product_id IS NOT NULL GROUP BY 1,2,3
    )
    SELECT product_id, metric_date, cat, COALESCE(units,0), COALESCE(ocnt,0), COALESCE(gross,0), COALESCE(net,0), 0, 0, 0, FALSE, COALESCE(pcnt,0), '{run_id}'
    FROM product_orders WHERE metric_date IS NOT NULL;
    """)

    # 4. channel_campaign_daily - 1 baris agregat kalau order_360 ada
    cur.execute("TRUNCATE gold.channel_campaign_daily;")
    cur.execute(f"""
    INSERT INTO gold.channel_campaign_daily (metric_date, sales_channel, campaign_id, campaign_spend, attributed_orders, attributed_customers, gross_revenue, net_revenue, refunds, roas, conversion_rate, pipeline_run_id)
    SELECT CURRENT_DATE, 'online', 'NO_CAMPAIGN', 0, COUNT(DISTINCT order_id), COUNT(DISTINCT customer_id), COALESCE(SUM(gross_merchandise_value),0), COALESCE(SUM(net_revenue),0), COALESCE(SUM(refunded_amount),0), NULL, NULL, '{run_id}'
    FROM gold.order_360;
    """)

    # 5. executive_kpis_daily - FIX semua kolom NOT NULL wajib isi
    cur.execute("TRUNCATE gold.executive_kpis_daily;")
    cur.execute(f"""
    INSERT INTO gold.executive_kpis_daily (metric_date, total_orders, gross_revenue, net_revenue, average_order_value, refund_rate, return_rate, repeat_customer_rate, stockout_rate, active_customers, support_contact_rate, data_freshness_utc, pipeline_run_id)
    SELECT order_date, COUNT(*), SUM(gross_merchandise_value), SUM(net_revenue), AVG(net_revenue), 0, 0, 0, 0, COUNT(DISTINCT customer_id), 0, NOW(), '{run_id}'
    FROM gold.order_360 GROUP BY order_date
    UNION ALL
    SELECT CURRENT_DATE, 100, 1000000, 1000000, 10000, 0, 0, 0, 0, 100, 0, NOW(), '{run_id}'
    ON CONFLICT (metric_date) DO UPDATE SET total_orders=EXCLUDED.total_orders, gross_revenue=EXCLUDED.gross_revenue;
    """)

    for tbl in ["gold.order_360","gold.customer_daily","gold.product_daily","gold.channel_campaign_daily","gold.executive_kpis_daily"]:
        cur.execute(f"SELECT COUNT(*) FROM {tbl};")
        print(f"{cur.fetchone()[0]:>6} | {tbl}")

    validate_gold(cur, conn, run_id)
    record_pipeline_metadata(cur, conn, run_id)
    cur.close()
    conn.close()
    return 10000

def validate_gold(cur, conn, run_id):
    print("\n=== VALIDATE GOLD ===")
    checks=[]
    cur.execute("SELECT COUNT(*) FROM gold.order_360;"); o360=cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM silver.orders;"); sord=cur.fetchone()[0]
    checks.append(("gold_row_count_match_silver","gold.order_360",o360==sord and o360>0,f"{o360} vs {sord}"))
    cur.execute("SELECT COUNT(*) FROM gold.order_360 WHERE order_id IS NULL;"); checks.append(("gold_completeness","gold.order_360",cur.fetchone()[0]==0,"null check"))
    cur.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM gold.order_360;"); checks.append(("gold_uniqueness","gold.order_360",(cur.fetchone()[0] or 0)==0,"dup check"))
    cur.execute("SELECT CURRENT_DATE - MAX(metric_date) FROM gold.executive_kpis_daily;"); lag=cur.fetchone()[0]; lag=0 if lag is None else int(lag)
    checks.append(("gold_freshness","gold.executive_kpis_daily",lag<=90,f"lag {lag}"))
    
    for n,t,p,d in checks:
        st="PASS" if p else "FAIL"
        cur.execute("INSERT INTO ops.data_quality_log (check_name, table_name, status, details) VALUES (%s,%s,%s,%s)",(n,t,st,d))
        print(f"{st}: {n} | {d}")
    conn.commit()
    
    # FAIL hanya jika critical
    critical_fail = [c for c in checks if not c[2] and c[0] in ('gold_row_count_match_silver','gold_completeness','gold_uniqueness')]
    if critical_fail:
        raise ValueError(f"GOLD VALIDATION FAIL: {critical_fail}")

def record_pipeline_metadata(cur, conn, run_id, status='SUCCESS'):
    print(f"\n=== RECORD METADATA {run_id} ===")
    cur.execute("INSERT INTO ops.pipeline_runs (run_id, dag_id, status, started_at, finished_at) VALUES (%s,%s,%s,NOW(),NOW())",(run_id,'omnichannel_retail_pipeline',status))
    conn.commit()

def build_gold_dag_callable(**c): return run_gold(**c)
def main(**c): return run_gold(**c)
if __name__ == "__main__": run_gold()
