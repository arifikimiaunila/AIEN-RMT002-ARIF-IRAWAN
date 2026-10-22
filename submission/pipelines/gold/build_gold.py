import psycopg2
from airflow.hooks.base import BaseHook

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

def run_gold(**context):
    print("=== START GOLD DAG CALLABLE ===")
    gold_sql = """
ALTER TABLE gold.order_360 ALTER COLUMN order_date DROP NOT NULL;
ALTER TABLE gold.customer_daily ALTER COLUMN gross_revenue DROP NOT NULL;
ALTER TABLE gold.customer_daily ALTER COLUMN gross_revenue SET DEFAULT 0;
ALTER TABLE gold.product_daily ALTER COLUMN gross_revenue DROP NOT NULL;
ALTER TABLE gold.product_daily ALTER COLUMN gross_revenue SET DEFAULT 0;

DO \$\$ DECLARE run_id TEXT := 'gold_' || to_char(NOW(),'YYYYMMDD_HH24MISS');
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
END \$\$;
"""

    conn = get_conn()
    cur = conn.cursor()
    print("=== VALIDASI SILVER ===")
    cur.execute("SELECT COUNT(*) FROM silver.orders")
    row = cur.fetchone()
    print(f"orders: {row[0] if row is not None else 0}")
    cur.execute("SELECT COUNT(*) FROM silver.order_items")
    row = cur.fetchone()
    print(f"order_items: {row[0] if row is not None else 0}")
    
    print("=== EXECUTE GOLD SQL ===")
    cur.execute(gold_sql)
    print("GOLD SQL SUCCESS")

    total = 0
    print("\\n=== FINAL BREAKDOWN GOLD ===")
    for tbl in ["gold.order_360", "gold.customer_daily", "gold.product_daily", "gold.channel_campaign_daily", "gold.executive_kpis_daily"]:
        cur.execute(f"SELECT COUNT(*) FROM {tbl};")
        row = cur.fetchone()
        cnt = row[0] if row is not None else 0
        print(f"{cnt:>6} | {tbl}")
        total += cnt

    cur.close()
    conn.close()
    print(f"\\nDONE Gold {total} rows")
    return total

def build_gold_dag_callable(**context):
    return run_gold(**context)

def main(**context):
    return run_gold(**context)

if __name__ == "__main__":
    run_gold()
