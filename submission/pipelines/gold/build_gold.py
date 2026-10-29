"""Build Gold marts from the typed Silver layer with strict idempotency and business contracts."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg2


def get_conn() -> psycopg2.extensions.connection:
    """Connect through multiple fallback Airflow connection IDs or fallback directly to Docker network host."""
    try:
        from airflow.exceptions import AirflowException
        from airflow.hooks.base import BaseHook
    except ModuleNotFoundError:
        return _connect_from_environment()

    for conn_id in ["retail_postgres", "postgres", "postgres_default", "airflow_db"]:
        try:
            airflow_connection = BaseHook.get_connection(conn_id)
            return psycopg2.connect(
                host=airflow_connection.host or os.getenv("POSTGRES_HOST", "postgres"),
                port=airflow_connection.port or int(os.getenv("POSTGRES_PORT", "5432")),
                dbname=airflow_connection.schema or os.getenv("POSTGRES_DB", "retail_analytics"),
                user=airflow_connection.login or os.getenv("POSTGRES_USER", "retail"),
                password=airflow_connection.password or os.getenv("POSTGRES_PASSWORD", "retail"),
            )
        except (AirflowException, Exception):
            continue

    return _connect_from_environment()


def _connect_from_environment() -> psycopg2.extensions.connection:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url)

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "retail_analytics"),
        user=os.getenv("POSTGRES_USER", "retail"),
        password=os.getenv("POSTGRES_PASSWORD", "retail"),
        sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
    )


def _build_order_360(cur: Any, run_id: str) -> int:
    """Build gold.order_360 with strict grain (one row per order_id) and UPSERT for idempotency[cite: 9]."""
    cur.execute(
        """
        INSERT INTO gold.order_360 (
            order_id, customer_id, order_date, sales_channel, store_id,
            gross_merchandise_value, discount_amount, shipping_revenue,
            captured_payment_amount, refunded_amount, net_revenue,
            item_count, unit_quantity, order_status, payment_status,
            return_status, promotion_count, first_order_flag, pipeline_run_id
        )
        WITH order_base AS (
            SELECT DISTINCT ON (o.order_id)
                   o.order_id, o.customer_id,
                   COALESCE(o.order_date, o.order_date_utc::date, o.ordered_at_utc::date) AS order_date,
                   o.amount AS total_amount,
                   COALESCE(NULLIF(o.sales_channel, ''), 'online') AS sales_channel,
                   o.store_id,
                   COALESCE(NULLIF(o.order_status, ''), NULLIF(o.status, ''), 'unknown') AS order_status
            FROM silver.orders o
            WHERE o.order_id IS NOT NULL
              AND o.customer_id IS NOT NULL
              AND COALESCE(o.order_date, o.order_date_utc::date, o.ordered_at_utc::date) IS NOT NULL
              AND COALESCE(o.is_current, TRUE)
            ORDER BY o.order_id, o.updated_at_utc DESC NULLS LAST, o.silver_processed_at DESC NULLS LAST
        ),
        item_totals AS (
            SELECT oi.order_id,
                   SUM(COALESCE(oi.quantity, 0) * COALESCE(oi.unit_price, 0)) AS gmv,
                   SUM(COALESCE(oi.item_discount_amount, oi.discount_amount, 0)) AS discount,
                   COUNT(*)::integer AS item_count,
                   SUM(COALESCE(oi.quantity, 0))::integer AS unit_quantity,
                   COUNT(DISTINCT oi.promotion_id)::integer AS item_promotion_count
            FROM silver.order_items oi
            WHERE oi.order_id IS NOT NULL AND COALESCE(oi.is_orphan, FALSE) = FALSE
            GROUP BY oi.order_id
        ),
        order_promotion_totals AS (
            SELECT op.order_id, 
                   COUNT(DISTINCT op.promotion_id)::integer AS promotion_count,
                   SUM(COALESCE(op.discount_amount, 0)) AS discount
            FROM silver.order_promotions op
            GROUP BY op.order_id
        ),
        payment_totals AS (
            SELECT order_id,
                   SUM(COALESCE(amount, 0)) FILTER (
                       WHERE lower(COALESCE(payment_status, '')) IN ('captured', 'paid', 'completed', 'success')
                   ) AS captured_amount,
                   CASE
                       WHEN BOOL_OR(lower(COALESCE(payment_status, '')) IN ('captured', 'paid', 'success')) THEN 'captured'
                       WHEN BOOL_OR(lower(COALESCE(payment_status, '')) = 'completed') THEN 'completed'
                       WHEN BOOL_OR(lower(COALESCE(payment_status, '')) = 'refunded') THEN 'refunded'
                       WHEN BOOL_OR(lower(COALESCE(payment_status, '')) = 'pending') THEN 'pending'
                       WHEN BOOL_OR(lower(COALESCE(payment_status, '')) = 'failed') THEN 'failed'
                       ELSE 'unknown'
                   END AS payment_status
            FROM silver.payment_events
            WHERE order_id IS NOT NULL
            GROUP BY order_id
        ),
        refund_totals AS (
            SELECT order_id, SUM(COALESCE(amount, 0)) AS refunded_amount
            FROM silver.refund_events
            WHERE order_id IS NOT NULL
            GROUP BY order_id
        ),
        return_orders AS (
            SELECT DISTINCT ON (order_id) order_id, event_type
            FROM silver.return_events
            WHERE order_id IS NOT NULL
            ORDER BY order_id, occurred_ts DESC NULLS LAST
        ),
        first_orders AS (
            SELECT customer_id, MIN(order_date) AS first_order_date
            FROM order_base
            GROUP BY customer_id
        )
        SELECT o.order_id, o.customer_id, o.order_date, o.sales_channel, o.store_id,
               COALESCE(i.gmv, o.total_amount, 0),
               CASE WHEN COALESCE(i.discount, 0) <> 0 THEN i.discount ELSE COALESCE(p.discount, 0) END,
               COALESCE(s.shipping_revenue, 0),
               COALESCE(pay.captured_amount, o.total_amount, 0),
               COALESCE(r.refunded_amount, 0),
               COALESCE(i.gmv, o.total_amount, 0)
                   - CASE WHEN COALESCE(i.discount, 0) <> 0 THEN i.discount ELSE COALESCE(p.discount, 0) END
                   + COALESCE(s.shipping_revenue, 0)
                   - COALESCE(r.refunded_amount, 0),
               COALESCE(i.item_count, 0),
               COALESCE(i.unit_quantity, 0),
               o.order_status,
               COALESCE(pay.payment_status, 'unknown'),
               COALESCE(ro.event_type, 'NONE') AS return_status,
               GREATEST(COALESCE(i.item_promotion_count, 0), COALESCE(p.promotion_count, 0)),
               o.order_date = f.first_order_date,
               %s
        FROM order_base o
        LEFT JOIN item_totals i ON i.order_id = o.order_id
        LEFT JOIN order_promotion_totals p ON p.order_id = o.order_id
        LEFT JOIN payment_totals pay ON pay.order_id = o.order_id
        LEFT JOIN refund_totals r ON r.order_id = o.order_id
        LEFT JOIN return_orders ro ON ro.order_id = o.order_id
        LEFT JOIN first_orders f ON f.customer_id = o.customer_id
        LEFT JOIN silver.orders s ON s.order_id = o.order_id AND COALESCE(s.is_current, TRUE)
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            order_date = EXCLUDED.order_date,
            sales_channel = EXCLUDED.sales_channel,
            store_id = EXCLUDED.store_id,
            gross_merchandise_value = EXCLUDED.gross_merchandise_value,
            discount_amount = EXCLUDED.discount_amount,
            shipping_revenue = EXCLUDED.shipping_revenue,
            captured_payment_amount = EXCLUDED.captured_payment_amount,
            refunded_amount = EXCLUDED.refunded_amount,
            net_revenue = EXCLUDED.net_revenue,
            item_count = EXCLUDED.item_count,
            unit_quantity = EXCLUDED.unit_quantity,
            order_status = EXCLUDED.order_status,
            payment_status = EXCLUDED.payment_status,
            return_status = EXCLUDED.return_status,
            promotion_count = EXCLUDED.promotion_count,
            first_order_flag = EXCLUDED.first_order_flag,
            pipeline_run_id = EXCLUDED.pipeline_run_id
        """,
        (run_id,),
    )
    return cur.rowcount


def _build_customer_daily(cur: Any, run_id: str) -> int:
    """Build gold.customer_daily with UPSERT idempotency[cite: 9]."""
    cur.execute(
        """
        INSERT INTO gold.customer_daily (
            customer_id, metric_date, order_count, unit_quantity, gross_revenue,
            net_revenue, refund_amount, return_count, support_contacts, active_channel,
            new_customer_flag, repeat_customer_flag, pipeline_run_id
        )
        WITH activity AS (
            SELECT customer_id, order_date AS metric_date,
                   COUNT(*)::integer AS order_count,
                   SUM(unit_quantity)::integer AS unit_quantity,
                   SUM(gross_merchandise_value) AS gross_revenue,
                   SUM(net_revenue) AS net_revenue,
                   SUM(refunded_amount) AS refund_amount,
                   COUNT(*) FILTER (WHERE return_status <> 'NONE' AND return_status IS NOT NULL)::integer AS return_count,
                   BOOL_OR(first_order_flag) AS new_customer_flag
            FROM gold.order_360
            GROUP BY customer_id, order_date
        ),
        channel_rank AS (
            SELECT customer_id, order_date, sales_channel,
                   ROW_NUMBER() OVER (
                       PARTITION BY customer_id, order_date
                       ORDER BY COUNT(*) DESC, sales_channel
                   ) AS channel_rank
            FROM gold.order_360
            GROUP BY customer_id, order_date, sales_channel
        ),
        support_daily AS (
            SELECT COALESCE(t.customer_id, o.customer_id) AS customer_id,
                   COALESCE(t.occurred_ts::date, t.source_ingested_ts::date, t.ingested_at_utc::date) AS metric_date,
                   COUNT(DISTINCT t.ticket_id)::integer AS support_contacts
            FROM silver.support_events t
            LEFT JOIN gold.order_360 o ON o.order_id = t.order_id
            WHERE COALESCE(t.customer_id, o.customer_id) IS NOT NULL
              AND COALESCE(t.occurred_ts, t.source_ingested_ts) IS NOT NULL
            GROUP BY 1, 2
        ),
        customer_days AS (
            SELECT customer_id, metric_date FROM activity
            UNION
            SELECT customer_id, metric_date FROM support_daily
        ),
        customer_history AS (
            SELECT customer_id, MIN(order_date) AS first_order_date
            FROM gold.order_360
            GROUP BY customer_id
        )
        SELECT d.customer_id, d.metric_date, 
               COALESCE(a.order_count, 0),
               COALESCE(a.unit_quantity, 0), 
               COALESCE(a.gross_revenue, 0),
               COALESCE(a.net_revenue, 0), 
               COALESCE(a.refund_amount, 0),
               COALESCE(a.return_count, 0), 
               COALESCE(s.support_contacts, 0),
               c.sales_channel, 
               COALESCE(a.new_customer_flag, FALSE),
               h.first_order_date IS NOT NULL AND d.metric_date > h.first_order_date,
               %s
        FROM customer_days d
        LEFT JOIN activity a ON a.customer_id = d.customer_id AND a.metric_date = d.metric_date
        LEFT JOIN channel_rank c ON c.customer_id = d.customer_id AND c.order_date = d.metric_date AND c.channel_rank = 1
        LEFT JOIN support_daily s ON s.customer_id = d.customer_id AND s.metric_date = d.metric_date
        LEFT JOIN customer_history h ON h.customer_id = d.customer_id
        ON CONFLICT (customer_id, metric_date) DO UPDATE SET
            order_count = EXCLUDED.order_count,
            unit_quantity = EXCLUDED.unit_quantity,
            gross_revenue = EXCLUDED.gross_revenue,
            net_revenue = EXCLUDED.net_revenue,
            refund_amount = EXCLUDED.refund_amount,
            return_count = EXCLUDED.return_count,
            support_contacts = EXCLUDED.support_contacts,
            active_channel = EXCLUDED.active_channel,
            new_customer_flag = EXCLUDED.new_customer_flag,
            repeat_customer_flag = EXCLUDED.repeat_customer_flag,
            pipeline_run_id = EXCLUDED.pipeline_run_id
        """,
        (run_id,),
    )
    return cur.rowcount


def _build_product_daily(cur: Any, run_id: str) -> int:
    """Build gold.product_daily with UPSERT idempotency[cite: 9]."""
    cur.execute(
        """
        INSERT INTO gold.product_daily (
            product_id, metric_date, active_category, units_sold, order_count,
            gross_revenue, net_revenue, refunded_units, available_inventory,
            reserved_inventory, stockout_flag, promotion_count, pipeline_run_id
        )
        WITH product_sales AS (
            SELECT oi.product_id, o.order_date AS metric_date,
                   SUM(oi.quantity)::integer AS units_sold,
                   COUNT(DISTINCT oi.order_id)::integer AS order_count,
                   SUM(oi.quantity * oi.unit_price) AS gross_revenue,
                   SUM(COALESCE(oi.item_discount_amount, oi.discount_amount, 0)) AS discount,
                   COUNT(DISTINCT oi.promotion_id)::integer AS promotion_count
            FROM silver.order_items oi
            JOIN gold.order_360 o ON o.order_id = oi.order_id
            WHERE oi.product_id IS NOT NULL
            GROUP BY oi.product_id, o.order_date
        ),
        product_inventory AS (
            SELECT product_id, snapshot_date_utc AS metric_date,
                   SUM(stock_quantity)::integer AS available_inventory,
                   SUM(stock_reserved)::integer AS reserved_inventory
            FROM silver.inventory_snapshots
            WHERE product_id IS NOT NULL AND snapshot_date_utc IS NOT NULL
            GROUP BY product_id, snapshot_date_utc
        ),
        product_days AS (
            SELECT product_id, metric_date FROM product_sales
            UNION
            SELECT product_id, metric_date FROM product_inventory
        )
        SELECT d.product_id, d.metric_date,
               COALESCE(p.category_name, 'Uncategorized'),
               COALESCE(s.units_sold, 0), 
               COALESCE(s.order_count, 0),
               COALESCE(s.gross_revenue, 0),
               COALESCE(s.gross_revenue, 0) - COALESCE(s.discount, 0),
               0,
               i.available_inventory,
               i.reserved_inventory,
               CASE WHEN i.available_inventory IS NULL THEN NULL ELSE i.available_inventory <= 0 END,
               COALESCE(s.promotion_count, 0),
               %s
        FROM product_days d
        LEFT JOIN product_sales s ON s.product_id = d.product_id AND s.metric_date = d.metric_date
        LEFT JOIN product_inventory i ON i.product_id = d.product_id AND i.metric_date = d.metric_date
        LEFT JOIN silver.products p ON p.product_id = d.product_id
        ON CONFLICT (product_id, metric_date) DO UPDATE SET
            active_category = EXCLUDED.active_category,
            units_sold = EXCLUDED.units_sold,
            order_count = EXCLUDED.order_count,
            gross_revenue = EXCLUDED.gross_revenue,
            net_revenue = EXCLUDED.net_revenue,
            refunded_units = EXCLUDED.refunded_units,
            available_inventory = EXCLUDED.available_inventory,
            reserved_inventory = EXCLUDED.reserved_inventory,
            stockout_flag = EXCLUDED.stockout_flag,
            promotion_count = EXCLUDED.promotion_count,
            pipeline_run_id = EXCLUDED.pipeline_run_id
        """,
        (run_id,),
    )
    return cur.rowcount


def _build_channel_campaign_daily(cur: Any, run_id: str) -> int:
    """Build gold.channel_campaign_daily with UPSERT idempotency[cite: 9]."""
    cur.execute(
        """
        INSERT INTO gold.channel_campaign_daily (
            metric_date, sales_channel, campaign_id, campaign_spend,
            attributed_orders, attributed_customers, gross_revenue,
            net_revenue, refunds, roas, conversion_rate, pipeline_run_id
        )
        WITH ranked_touch AS (
            SELECT e.order_id, e.campaign_id, 
                   COALESCE(NULLIF(e.sales_channel, ''), e.channel, 'online') AS sales_channel,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.order_id
                       ORDER BY COALESCE(e.event_timestamp_utc, e.occurred_ts) DESC NULLS LAST, e.event_id
                   ) AS touch_rank
            FROM silver.web_events e
            JOIN gold.order_360 o ON o.order_id = e.order_id
            WHERE e.campaign_id IS NOT NULL
        ),
        campaign_sessions AS (
            SELECT COALESCE(e.event_timestamp_utc::date, e.occurred_ts::date, e.source_ingested_ts::date) AS metric_date,
                   COALESCE(NULLIF(e.sales_channel, ''), NULLIF(e.channel, ''), 'online') AS sales_channel,
                   COALESCE(NULLIF(e.campaign_id, ''), 'NO_CAMPAIGN') AS campaign_id,
                   COUNT(DISTINCT e.session_id)::integer AS session_count
            FROM silver.web_events e
            WHERE COALESCE(e.event_timestamp_utc, e.occurred_ts, e.source_ingested_ts) IS NOT NULL
            GROUP BY 1, 2, 3
        ),
        attributed AS (
            SELECT o.order_date AS metric_date,
                   COALESCE(NULLIF(t.sales_channel, ''), o.sales_channel) AS sales_channel,
                   t.campaign_id,
                   COUNT(*)::integer AS attributed_orders,
                   COUNT(DISTINCT o.customer_id)::integer AS attributed_customers,
                   SUM(o.gross_merchandise_value) AS gross_revenue,
                   SUM(o.net_revenue) AS net_revenue,
                   SUM(o.refunded_amount) AS refunds
            FROM gold.order_360 o
            JOIN ranked_touch t ON t.order_id = o.order_id AND t.touch_rank = 1
            GROUP BY o.order_date, COALESCE(NULLIF(t.sales_channel, ''), o.sales_channel), t.campaign_id
        ),
        unattributed AS (
            SELECT o.order_date AS metric_date, o.sales_channel,
                   'NO_CAMPAIGN'::text AS campaign_id,
                   COUNT(*)::integer AS attributed_orders,
                   COUNT(DISTINCT o.customer_id)::integer AS attributed_customers,
                   SUM(o.gross_merchandise_value) AS gross_revenue,
                   SUM(o.net_revenue) AS net_revenue,
                   SUM(o.refunded_amount) AS refunds
            FROM gold.order_360 o
            WHERE NOT EXISTS (
                SELECT 1 FROM ranked_touch t
                WHERE t.order_id = o.order_id AND t.touch_rank = 1
            )
            GROUP BY o.order_date, o.sales_channel
        ),
        campaign_sales AS (
            SELECT * FROM attributed
            UNION ALL
            SELECT * FROM unattributed
        ),
        daily_spend AS (
            SELECT s.spend_date_utc AS metric_date,
                   COALESCE(NULLIF(c.channel_name, ''), NULLIF(s.channel_id, ''), 'online') AS sales_channel,
                   s.campaign_id,
                   SUM(s.spend_amount) AS campaign_spend
            FROM silver.campaign_spend s
            LEFT JOIN silver.sales_channels c ON c.channel_id = s.channel_id
            GROUP BY s.spend_date_utc, COALESCE(NULLIF(c.channel_name, ''), NULLIF(s.channel_id, ''), 'online'), s.campaign_id
        ),
        campaign_keys AS (
            SELECT metric_date, sales_channel, campaign_id FROM campaign_sales
            UNION
            SELECT metric_date, sales_channel, campaign_id FROM daily_spend
            UNION
            SELECT metric_date, sales_channel, campaign_id FROM campaign_sessions
        )
        SELECT k.metric_date, k.sales_channel, k.campaign_id,
               COALESCE(s.campaign_spend, 0),
               COALESCE(a.attributed_orders, 0),
               COALESCE(a.attributed_customers, 0),
               COALESCE(a.gross_revenue, 0),
               COALESCE(a.net_revenue, 0),
               COALESCE(a.refunds, 0),
               CASE WHEN COALESCE(s.campaign_spend, 0) > 0 THEN a.net_revenue / s.campaign_spend END,
               CASE WHEN COALESCE(a.attributed_orders, 0) > 0 THEN a.attributed_orders::numeric / NULLIF(cs.session_count, 0) END,
               %s
        FROM campaign_keys k
        LEFT JOIN campaign_sales a ON a.metric_date = k.metric_date AND a.sales_channel = k.sales_channel AND a.campaign_id = k.campaign_id
        LEFT JOIN daily_spend s ON s.metric_date = k.metric_date AND s.sales_channel = k.sales_channel AND s.campaign_id = k.campaign_id
        LEFT JOIN campaign_sessions cs ON cs.metric_date = k.metric_date AND cs.sales_channel = k.sales_channel AND cs.campaign_id = k.campaign_id
        ON CONFLICT (metric_date, sales_channel, campaign_id) DO UPDATE SET
            campaign_spend = EXCLUDED.campaign_spend,
            attributed_orders = EXCLUDED.attributed_orders,
            attributed_customers = EXCLUDED.attributed_customers,
            gross_revenue = EXCLUDED.gross_revenue,
            net_revenue = EXCLUDED.net_revenue,
            refunds = EXCLUDED.refunds,
            roas = EXCLUDED.roas,
            conversion_rate = EXCLUDED.conversion_rate,
            pipeline_run_id = EXCLUDED.pipeline_run_id
        """,
        (run_id,),
    )
    return cur.rowcount


def _build_executive_kpis(cur: Any, run_id: str) -> int:
    """Build gold.executive_kpis_daily with UPSERT idempotency[cite: 9]."""
    cur.execute(
        """
        INSERT INTO gold.executive_kpis_daily (
            metric_date, total_orders, gross_revenue, net_revenue,
            average_order_value, refund_rate, return_rate, repeat_customer_rate,
            stockout_rate, active_customers, support_contact_rate,
            data_freshness_utc, pipeline_run_id
        )
        WITH order_daily AS (
            SELECT order_date AS metric_date,
                   COUNT(*)::integer AS total_orders,
                   SUM(gross_merchandise_value) AS gross_revenue,
                   SUM(net_revenue) AS net_revenue,
                   AVG(net_revenue) AS average_order_value,
                   COUNT(*) FILTER (WHERE captured_payment_amount > 0 AND refunded_amount > 0)::numeric
                   / NULLIF(COUNT(*) FILTER (WHERE captured_payment_amount > 0), 0) AS refund_rate,
                   COUNT(*) FILTER (WHERE return_status <> 'NONE' AND return_status IS NOT NULL)::numeric
                   / NULLIF(COUNT(*) FILTER (WHERE lower(order_status) NOT IN ('pending', 'unknown', 'failed')), 0) AS return_rate,
                   COUNT(DISTINCT customer_id) FILTER (WHERE first_order_flag = FALSE)::numeric
                   / NULLIF(COUNT(DISTINCT customer_id), 0) AS repeat_customer_rate,
                   COUNT(DISTINCT customer_id)::integer AS active_customers
            FROM gold.order_360
            GROUP BY order_date
        ),
        product_daily AS (
            SELECT metric_date, AVG(stockout_flag::integer)::numeric AS stockout_rate
            FROM gold.product_daily
            WHERE stockout_flag IS NOT NULL
            GROUP BY metric_date
        ),
        support_daily AS (
            SELECT metric_date, SUM(support_contacts)::numeric AS support_contacts
            FROM gold.customer_daily
            GROUP BY metric_date
        ),
        freshness AS (
            SELECT GREATEST(
                COALESCE((SELECT MAX(silver_processed_at) FROM silver.orders), '-infinity'::timestamptz),
                COALESCE((SELECT MAX(silver_processed_at) FROM silver.order_items), '-infinity'::timestamptz),
                COALESCE((SELECT MAX(silver_processed_at) FROM silver.payment_events), '-infinity'::timestamptz),
                COALESCE((SELECT MAX(silver_processed_at) FROM silver.refund_events), '-infinity'::timestamptz),
                COALESCE((SELECT MAX(silver_processed_at) FROM silver.inventory_snapshots), '-infinity'::timestamptz)
            ) AS freshness_utc
        )
        SELECT o.metric_date, o.total_orders, o.gross_revenue, o.net_revenue,
               o.average_order_value,
               COALESCE(o.refund_rate, 0), 
               COALESCE(o.return_rate, 0),
               COALESCE(o.repeat_customer_rate, 0), 
               COALESCE(p.stockout_rate, 0),
               o.active_customers,
               COALESCE(s.support_contacts / NULLIF(o.total_orders, 0), 0),
               COALESCE(NULLIF(f.freshness_utc, '-infinity'::timestamptz), NOW()),
               %s
        FROM order_daily o
        LEFT JOIN product_daily p ON p.metric_date = o.metric_date
        LEFT JOIN support_daily s ON s.metric_date = o.metric_date
        CROSS JOIN freshness f
        ON CONFLICT (metric_date) DO UPDATE SET
            total_orders = EXCLUDED.total_orders,
            gross_revenue = EXCLUDED.gross_revenue,
            net_revenue = EXCLUDED.net_revenue,
            average_order_value = EXCLUDED.average_order_value,
            refund_rate = EXCLUDED.refund_rate,
            return_rate = EXCLUDED.return_rate,
            repeat_customer_rate = EXCLUDED.repeat_customer_rate,
            stockout_rate = EXCLUDED.stockout_rate,
            active_customers = EXCLUDED.active_customers,
            support_contact_rate = EXCLUDED.support_contact_rate,
            data_freshness_utc = EXCLUDED.data_freshness_utc,
            pipeline_run_id = EXCLUDED.pipeline_run_id
        """,
        (run_id,),
    )
    return cur.rowcount


def validate_gold(cur: Any, conn: Any, run_id: str) -> None:
    """Validate metric contracts and revenue reconciliation formulas[cite: 9]."""
    del conn
    cur.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM gold.order_360),
            (SELECT COUNT(*) FROM silver.orders
             WHERE order_id IS NOT NULL AND customer_id IS NOT NULL
               AND COALESCE(order_date, order_date_utc::date, ordered_at_utc::date) IS NOT NULL
               AND COALESCE(is_current, TRUE)),
            (SELECT COUNT(*) FROM gold.order_360
             WHERE net_revenue <> gross_merchandise_value - discount_amount + shipping_revenue - refunded_amount),
            (SELECT COUNT(*) FROM (
                SELECT d.customer_id, d.metric_date
                FROM gold.customer_daily d
                LEFT JOIN (
                    SELECT customer_id, order_date,
                           SUM(gross_merchandise_value) AS gross_revenue,
                           SUM(net_revenue) AS net_revenue,
                           SUM(refunded_amount) AS refund_amount
                    FROM gold.order_360
                    GROUP BY customer_id, order_date
                ) o ON o.customer_id = d.customer_id AND o.order_date = d.metric_date
                WHERE COALESCE(d.gross_revenue, 0) <> COALESCE(o.gross_revenue, 0)
                   OR COALESCE(d.net_revenue, 0) <> COALESCE(o.net_revenue, 0)
                   OR COALESCE(d.refund_amount, 0) <> COALESCE(o.refund_amount, 0)
            ) invalid_customer_rows)
        """
    )
    gold_orders, eligible_orders, invalid_order_revenue, invalid_customer_revenue = cur.fetchone()
    
    if gold_orders == 0:
        raise ValueError("Gold order_360 is empty; refusing to report a successful run.")
    if gold_orders != eligible_orders:
        raise ValueError(
            f"Gold order_360 count ({gold_orders}) does not match eligible Silver orders ({eligible_orders})."
        )
    if invalid_order_revenue or invalid_customer_revenue:
        raise ValueError(
            "Gold revenue reconciliation failed: "
            f"{invalid_order_revenue} order rows and "
            f"{invalid_customer_revenue} customer rows differ from the revenue formula."
        )

    cur.execute(
        """
        INSERT INTO ops.data_quality_log (check_name, table_name, status, details)
        VALUES (%s, %s, %s, %s)
        """,
        (
            "gold_reconciliation",
            "gold",
            "PASS",
            f"run_id={run_id}; orders={gold_orders}; eligible_orders={eligible_orders}",
        ),
    )


def record_pipeline_metadata(cur: Any, run_id: str, status: str = "SUCCESS") -> None:
    """Record final metadata status to ops.pipeline_runs[cite: 9]."""
    cur.execute(
        """
        INSERT INTO ops.pipeline_runs (run_id, dag_id, status, started_at, finished_at)
        VALUES (%s, %s, %s, NOW(), NOW())
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            finished_at = EXCLUDED.finished_at
        """,
        (run_id, "omnichannel_retail_pipeline", status),
    )


def run_gold(**context: Any) -> dict[str, int | str]:
    """Atomically build each Gold mart using idempotent upserts and validate published metrics[cite: 9]."""
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"gold_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
                
                counts = {
                    "order_360": _build_order_360(cur, run_id),
                    "customer_daily": _build_customer_daily(cur, run_id),
                    "product_daily": _build_product_daily(cur, run_id),
                    "channel_campaign_daily": _build_channel_campaign_daily(cur, run_id),
                    "executive_kpis_daily": _build_executive_kpis(cur, run_id),
                }
                validate_gold(cur, conn, run_id)
                record_pipeline_metadata(cur, run_id)

        result: dict[str, int | str] = {"status": "SUCCESS", "run_id": run_id, **counts}
        print(f"Gold refresh complete: {result}")
        return result
    except Exception as err:
        conn.rollback()
        print(f"🚨 Error building Gold layer: {err}")
        raise
    finally:
        conn.close()


def build_gold_dag_callable(**context: Any) -> dict[str, int | str]:
    return run_gold(**context)


def main(**context: Any) -> dict[str, int | str]:
    return run_gold(**context)


if __name__ == "__main__":
    run_gold()
