import psycopg2
import traceback

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

def stage2_fill_silver():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SET session_replication_role = replica;")
    try:
        print(">> STAGE 2: Memulai pencatatan dedup log & pengisian layer Silver")

        # === PROSES PENCATATAN DUPLIKASI KE OPS.SILVER_DEDUP_LOG ===
        cur.execute("TRUNCATE TABLE ops.silver_dedup_log RESTART IDENTITY;")
        conn.commit()

        cur.execute("""
        WITH ranked_lineage AS (
            SELECT 
                lineage_id,
                target_table,
                deterministic_key,
                bronze_source_record_id AS source_record_id,
                payment_id,
                event_id,
                order_id,
                address_id,
                city_id,
                product_id, 
                promotion_id,
                channel_id,
                store_id,
                campaign_id,
                file_name,
                bronze_row_number AS row_number,
                ROW_NUMBER() OVER (
                    PARTITION BY deterministic_key 
                    ORDER BY recorded_at ASC, lineage_id ASC
                ) AS rn,
                COUNT(*) OVER (
                    PARTITION BY deterministic_key
                ) as total_dupes
            FROM ops.silver_lineage
        )
        INSERT INTO ops.silver_dedup_log (
            target_table, deterministic_key, source_record_id, payment_id,
            event_id, order_id, address_id, city_id, product_id, 
            promotion_id, channel_id, store_id, campaign_id, file_name,
            row_number, is_kept, duplicate_count, processed_at
        )
        SELECT 
            target_table, deterministic_key, source_record_id, payment_id,
            event_id, order_id, address_id, city_id, product_id, 
            promotion_id, channel_id, store_id, campaign_id, file_name,
            row_number,
            CASE WHEN rn = 1 THEN TRUE ELSE FALSE END AS is_kept,
            total_dupes AS duplicate_count,
            NOW() AS processed_at
        FROM ranked_lineage;
        """)
        conn.commit()
        print(">> Selesai mencatat log duplikasi ke ops.silver_dedup_log")

        def safe_insert(name, lineage_sql, fallback_sql=None):
            cur.execute(lineage_sql)
            cnt = cur.rowcount
            print(f"{name:30} | lineage {cnt}")
            conn.commit()
            if cnt == 0 and fallback_sql:
                print(f"  -> {name} kosong dari lineage, coba fallback bronze...")
                try:
                    cur.execute(fallback_sql)
                    print(f"{name:30} | fallback bronze {cur.rowcount}")
                    conn.commit()
                except Exception as e2:
                    print(f"{name} fallback FAIL: {e2}")
                    conn.rollback()
                    cur.execute("SET session_replication_role = replica;")

        # 1. CUSTOMERS
        cur.execute("TRUNCATE silver.customers RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.customers', """
        WITH base_customers AS (
            SELECT 
                customer_id, city_id,
                (payload->>'email') AS email,
                COALESCE(payload->>'customer_name', payload->>'full_name') AS full_name,
                (payload->>'phone') AS phone,
                payload->>'customer_segment' AS customer_segment,
                NULLIF(payload->>'created_at_utc', '')::TIMESTAMPTZ AS created_at_utc,
                NULLIF(payload->>'valid_from_utc', '')::TIMESTAMPTZ AS valid_from_utc,
                NULLIF(payload->>'valid_to_utc', '')::TIMESTAMPTZ AS valid_to_utc,
                NULLIF(payload->>'valid_from_utc', '')::TIMESTAMPTZ AS valid_from_ts,
                NULLIF(payload->>'valid_to_utc', '')::TIMESTAMPTZ AS valid_to_ts,
                payload->>'source_row_id' AS source_row_id,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.customers'
              AND customer_id IS NOT NULL
        )
        INSERT INTO silver.customers (
            customer_id, email, full_name, phone, city_id, customer_segment,
            created_at_utc, valid_from_utc, valid_to_utc, valid_from_ts, valid_to_ts,
            source_row_id, bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, effective_from,
            effective_to, version_number, is_orphan, deterministic_key
        )
        SELECT 
            customer_id, email, full_name, phone, city_id, customer_segment,
            created_at_utc, valid_from_utc, valid_to_utc, valid_from_ts, valid_to_ts,
            source_row_id, bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, CURRENT_TIMESTAMP,
            NULL, 1, FALSE, deterministic_key
        FROM base_customers
        ON CONFLICT (customer_id) DO UPDATE 
        SET 
            email = EXCLUDED.email, full_name = EXCLUDED.full_name, phone = EXCLUDED.phone,
            city_id = EXCLUDED.city_id, customer_segment = EXCLUDED.customer_segment,
            valid_from_utc = EXCLUDED.valid_from_utc, valid_to_utc = EXCLUDED.valid_to_utc,
            valid_from_ts = EXCLUDED.valid_from_ts, valid_to_ts = EXCLUDED.valid_to_ts,
            source_row_id = EXCLUDED.source_row_id, bronze_file_name = EXCLUDED.bronze_file_name,
            bronze_row_number = EXCLUDED.bronze_row_number, bronze_source_record_id = EXCLUDED.bronze_source_record_id,
            bronze_checksum = EXCLUDED.bronze_checksum, silver_processed_at = CURRENT_TIMESTAMP,
            deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 1B. CUSTOMER_ADDRESSES
        cur.execute("TRUNCATE silver.customer_addresses RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.customer_addresses', """
        WITH base_addresses AS (
            SELECT 
                address_id, customer_id, city_id,
                NULLIF(payload->>'valid_from_utc', '')::TIMESTAMPTZ AS valid_from_utc,
                NULLIF(payload->>'valid_to_utc', '')::TIMESTAMPTZ AS valid_to_utc,
                NULLIF(payload->>'valid_from_utc', '')::TIMESTAMPTZ AS valid_from_ts,
                NULLIF(payload->>'valid_to_utc', '')::TIMESTAMPTZ AS valid_to_ts,
                payload->>'source_row_id' AS source_row_id,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.customer_addresses'
              AND address_id IS NOT NULL
        )
        INSERT INTO silver.customer_addresses (
            address_id, customer_id, city_id, valid_from_utc, valid_to_utc,
            valid_from_ts, valid_to_ts, source_row_id, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            silver_processed_at, is_current, effective_from, effective_to,
            version_number, is_orphan, deterministic_key
        )
        SELECT 
            address_id, customer_id, city_id, valid_from_utc, valid_to_utc,
            valid_from_ts, valid_to_ts, source_row_id, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            CURRENT_TIMESTAMP, TRUE, CURRENT_TIMESTAMP, NULL, 1, FALSE, deterministic_key
        FROM base_addresses
        ON CONFLICT (address_id) DO UPDATE 
        SET 
            customer_id = EXCLUDED.customer_id, city_id = EXCLUDED.city_id,
            valid_from_utc = EXCLUDED.valid_from_utc, valid_to_utc = EXCLUDED.valid_to_utc,
            valid_from_ts = EXCLUDED.valid_from_ts, valid_to_ts = EXCLUDED.valid_to_ts,
            source_row_id = EXCLUDED.source_row_id, bronze_file_name = EXCLUDED.bronze_file_name,
            bronze_row_number = EXCLUDED.bronze_row_number, bronze_source_record_id = EXCLUDED.bronze_source_record_id,
            bronze_checksum = EXCLUDED.bronze_checksum, silver_processed_at = CURRENT_TIMESTAMP,
            deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 1C. CUSTOMER_PROFILES
        cur.execute("TRUNCATE silver.customer_profiles RESTART IDENTITY CASCADE;")
        conn.commit()  
        safe_insert('silver.customer_profiles', """
        WITH base_profiles AS (
            SELECT 
                COALESCE(source_record_id, customer_id, 'PRF-' || bronze_id) AS profile_id,
                customer_id, city_id, customer_segment,
                payload->>'valid_from_utc' AS valid_from_utc,
                payload->>'valid_to_utc' AS valid_to_utc,
                NULLIF(payload->>'valid_from_utc', '')::TIMESTAMPTZ AS valid_from_ts,
                NULLIF(payload->>'valid_to_utc', '')::TIMESTAMPTZ AS valid_to_ts,
                payload->>'source_row_id' AS source_row_id,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.customer_profiles'
              AND customer_id IS NOT NULL
        )
        INSERT INTO silver.customer_profiles (
            profile_id, customer_id, city_id, customer_segment, valid_from_utc,
            valid_to_utc, valid_from_ts, valid_to_ts, source_row_id, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            profile_id, customer_id, city_id, customer_segment, valid_from_utc,
            valid_to_utc, valid_from_ts, valid_to_ts, source_row_id, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM base_profiles
        ON CONFLICT (profile_id) DO UPDATE 
        SET 
            customer_id = EXCLUDED.customer_id, city_id = EXCLUDED.city_id,
            customer_segment = EXCLUDED.customer_segment, valid_from_utc = EXCLUDED.valid_from_utc,
            valid_to_utc = EXCLUDED.valid_to_utc, valid_from_ts = EXCLUDED.valid_from_ts,
            valid_to_ts = EXCLUDED.valid_to_ts, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 2. CITY_REFERENCE
        cur.execute("TRUNCATE silver.city_reference RESTART IDENTITY CASCADE;")
        conn.commit()  
        safe_insert('silver.city_reference', """
        WITH base_cities AS (
            SELECT 
                city_id,
                COALESCE(city_name, payload->>'city_name') AS city_name,
                COALESCE(country, payload->>'country') AS country,
                payload->>'source_row_id' AS source_row_id,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.city_reference'
              AND city_id IS NOT NULL
        )
        INSERT INTO silver.city_reference (
            city_id, city_name, country, source_row_id, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            city_id, city_name, country, source_row_id, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM base_cities
        ON CONFLICT (city_id) DO UPDATE 
        SET 
            city_name = EXCLUDED.city_name, country = EXCLUDED.country,
            source_row_id = EXCLUDED.source_row_id, bronze_file_name = EXCLUDED.bronze_file_name,
            bronze_row_number = EXCLUDED.bronze_row_number, bronze_source_record_id = EXCLUDED.bronze_source_record_id,
            bronze_checksum = EXCLUDED.bronze_checksum, silver_processed_at = CURRENT_TIMESTAMP,
            deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 3. PRODUCTS
        cur.execute("TRUNCATE silver.products RESTART IDENTITY CASCADE;")
        conn.commit()  
        safe_insert('silver.products', """
        WITH base_products AS (
            SELECT 
                product_id,
                COALESCE(sku, payload->>'sku') AS sku,
                COALESCE(product_name, payload->>'product_name') AS product_name,
                category_id, category_name,
                COALESCE(unit_price, (payload->>'price')::numeric, 0) AS price,
                unit_price,
                payload->>'source_row_id' AS source_row_id,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.products'
              AND product_id IS NOT NULL
        )
        INSERT INTO silver.products (
            product_id, sku, product_name, category_id, category_name, price,
            unit_price, source_row_id, bronze_file_name, bronze_row_number,
            bronze_source_record_id, bronze_checksum, silver_processed_at,
            is_current, deterministic_key
        )
        SELECT 
            product_id, sku, product_name, category_id, category_name, price,
            unit_price, source_row_id, bronze_file_name, bronze_row_number,
            bronze_source_record_id, bronze_checksum, CURRENT_TIMESTAMP,
            TRUE, deterministic_key
        FROM base_products
        ON CONFLICT (product_id) DO UPDATE 
        SET 
            sku = EXCLUDED.sku, product_name = EXCLUDED.product_name,
            category_id = EXCLUDED.category_id, category_name = EXCLUDED.category_name,
            price = EXCLUDED.price, unit_price = EXCLUDED.unit_price,
            source_row_id = EXCLUDED.source_row_id, bronze_file_name = EXCLUDED.bronze_file_name,
            bronze_row_number = EXCLUDED.bronze_row_number, bronze_source_record_id = EXCLUDED.bronze_source_record_id,
            bronze_checksum = EXCLUDED.bronze_checksum, silver_processed_at = CURRENT_TIMESTAMP,
            deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 3B. PRODUCT_CATEGORIES
        cur.execute("TRUNCATE silver.product_categories RESTART IDENTITY CASCADE;")
        conn.commit()  
        safe_insert('silver.product_categories', """
        WITH base_product_categories AS (
            SELECT 
                product_id, category_id,
                COALESCE(category_name, payload->>'category_name') AS category_name,
                NULLIF(payload->>'valid_from_utc', '')::TIMESTAMPTZ AS valid_from_utc,
                NULLIF(payload->>'valid_to_utc', '')::TIMESTAMPTZ AS valid_to_utc,
                NULLIF(payload->>'valid_from_utc', '')::TIMESTAMPTZ AS valid_from_ts,
                NULLIF(payload->>'valid_to_utc', '')::TIMESTAMPTZ AS valid_to_ts,
                payload->>'source_row_id' AS source_row_id,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.product_categories'
              AND product_id IS NOT NULL
              AND category_id IS NOT NULL
        )
        INSERT INTO silver.product_categories (
            product_id, category_id, category_name, valid_from_utc, valid_to_utc,
            valid_from_ts, valid_to_ts, source_row_id, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            product_id, category_id, category_name, valid_from_utc, valid_to_utc,
            valid_from_ts, valid_to_ts, source_row_id, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM base_product_categories
        ON CONFLICT (product_id, category_id) DO UPDATE 
        SET 
            category_name = EXCLUDED.category_name, valid_from_utc = EXCLUDED.valid_from_utc,
            valid_to_utc = EXCLUDED.valid_to_utc, valid_from_ts = EXCLUDED.valid_from_ts,
            valid_to_ts = EXCLUDED.valid_to_ts, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 4. STORES
        cur.execute("TRUNCATE silver.stores RESTART IDENTITY CASCADE;")
        conn.commit()  
        safe_insert('silver.stores', """
        WITH base_stores AS (
            SELECT 
                store_id,
                COALESCE(store_name, payload->>'store_name') AS store_name,
                city_id,
                COALESCE(location_type, payload->>'location_type') AS location_type,
                payload->>'source_row_id' AS source_row_id,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.stores'
              AND store_id IS NOT NULL
        )
        INSERT INTO silver.stores (
            store_id, store_name, city_id, location_type, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            store_id, store_name, city_id, location_type, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM base_stores
        ON CONFLICT (store_id) DO UPDATE 
        SET 
            store_name = EXCLUDED.store_name, city_id = EXCLUDED.city_id,
            location_type = EXCLUDED.location_type, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 5. SALES_CHANNELS
        cur.execute("TRUNCATE silver.sales_channels RESTART IDENTITY CASCADE;")
        conn.commit()  
        safe_insert('silver.sales_channels', """
        WITH base_channels AS (
            SELECT 
                channel_id,
                COALESCE(channel_name, payload->>'channel_name', payload->>'channel') AS channel_name,
                payload->>'source_row_id' AS source_row_id,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.sales_channels'
              AND channel_id IS NOT NULL
        )
        INSERT INTO silver.sales_channels (
            channel_id, channel_name, source_row_id, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            channel_id, channel_name, source_row_id, bronze_file_name,
            bronze_row_number, bronze_source_record_id, bronze_checksum,
            CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM base_channels
        ON CONFLICT (channel_id) DO UPDATE 
        SET 
            channel_name = EXCLUDED.channel_name, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 6. ORDERS (DIPERBAIKI DENGAN DISTINCT ON)
        cur.execute("TRUNCATE silver.orders RESTART IDENTITY CASCADE;")
        conn.commit()  
        safe_insert('silver.orders', """
        WITH base_orders AS (
            SELECT 
                b.order_id, b.customer_id, b.store_id, b.channel_id, b.sales_channel,
                b.status, b.amount, b.shipping_revenue, b.event_type, b.payload,
                b.source_record_id, b.file_name, b.row_number, b.checksum, b.ingested_at, b.target_table,
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
              AND (b.order_id IS NOT NULL OR b.payload->>'order_id' IS NOT NULL)
        ),
        normalized_orders AS (
            SELECT 
                bo.*,
                COALESCE(bo.order_id, bo.payload->>'order_id') AS norm_order_id,
                COALESCE(
                    sm.normalized_status,
                    CASE 
                        WHEN bo.raw_status_text IN ('paid', 'completed', 'success') THEN 'completed'
                        WHEN bo.raw_status_text IN ('pending', 'processing') THEN 'pending'
                        WHEN bo.raw_status_text IN ('cancelled', 'canceled') THEN 'cancelled'
                        WHEN bo.raw_status_text IN ('failed') THEN 'failed'
                        WHEN bo.raw_status_text IN ('refunded') THEN 'refunded'
                        WHEN bo.raw_status_text IN ('returned', 'return') THEN 'returned'
                        ELSE 'pending'
                    END
                ) AS norm_status
            FROM base_orders bo
            LEFT JOIN ops.silver_status_map sm 
                ON bo.raw_status_text = sm.raw_status
        ),
        deduplicated_orders AS (
            SELECT DISTINCT ON (norm_order_id)
                norm_order_id AS order_id,
                COALESCE(customer_id, payload->>'customer_id') AS customer_id,
                COALESCE(store_id, payload->>'store_id') AS store_id,
                COALESCE(channel_id, payload->>'channel_id') AS channel_id,
                COALESCE(sales_channel, payload->>'sales_channel', 'online') AS sales_channel,
                norm_status AS order_status,
                norm_status AS status,
                NULLIF(payload->>'order_date', '')::DATE AS order_date,
                NULLIF(payload->>'order_date_utc', '')::TIMESTAMPTZ AS order_date_utc,
                NULLIF(payload->>'ordered_at_utc', '')::TIMESTAMPTZ AS ordered_at_utc,
                NULLIF(payload->>'updated_at_utc', '')::TIMESTAMPTZ AS updated_at_utc,
                payload->>'updated_at_utc' AS updated_at_text,
                COALESCE(amount, (payload->>'amount')::numeric, 0) AS amount,
                COALESCE((payload->>'shipping_cost')::numeric, 0) AS shipping_cost,
                COALESCE(shipping_revenue, (payload->>'shipping_revenue')::numeric, 0) AS shipping_revenue,
                COALESCE(event_type, payload->>'event_type', 'NONE') AS event_type,
                payload->>'source_row_id' AS source_row_id,
                file_name AS bronze_file_name,
                row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id,
                checksum AS bronze_checksum,
                computed_deterministic_key AS deterministic_key
            FROM normalized_orders
            WHERE norm_order_id IS NOT NULL AND TRIM(norm_order_id) <> ''
            ORDER BY norm_order_id, ingested_at DESC
        )
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
        FROM deduplicated_orders
        ON CONFLICT (order_id) DO UPDATE 
        SET 
            customer_id = EXCLUDED.customer_id, store_id = EXCLUDED.store_id,
            channel_id = EXCLUDED.channel_id, sales_channel = EXCLUDED.sales_channel,
            order_status = EXCLUDED.order_status, status = EXCLUDED.status,
            order_date = EXCLUDED.order_date, order_date_utc = EXCLUDED.order_date_utc,
            ordered_at_utc = EXCLUDED.ordered_at_utc, updated_at_utc = EXCLUDED.updated_at_utc,
            updated_at_text = EXCLUDED.updated_at_text, amount = EXCLUDED.amount,
            shipping_cost = EXCLUDED.shipping_cost, shipping_revenue = EXCLUDED.shipping_revenue,
            event_type = EXCLUDED.event_type, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 6B. ORDER ITEMS 
        cur.execute("TRUNCATE silver.order_items RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.order_items', """
        WITH base_order_items AS (
            SELECT 
                order_item_id, order_id, product_id, promotion_id,
                quantity, unit_price, discount_amount, sales_channel, payload,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                ingested_at,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.order_items'
              AND order_item_id IS NOT NULL
        ),
        deduplicated_order_items AS (
            SELECT DISTINCT ON (order_item_id)
                order_item_id, order_id, product_id, promotion_id,
                COALESCE(quantity, (payload->>'quantity')::numeric, 0) AS quantity,
                COALESCE(unit_price, (payload->>'unit_price')::numeric, 0) AS unit_price,
                COALESCE((payload->>'item_discount_amount')::numeric, 0) AS item_discount_amount,
                COALESCE(discount_amount, (payload->>'discount_amount')::int, 0) AS discount_amount,
                COALESCE(
                    (payload->>'subtotal')::numeric, 
                    (COALESCE(quantity, (payload->>'quantity')::numeric, 0) * COALESCE(unit_price, (payload->>'unit_price')::numeric, 0))
                ) AS subtotal,
                COALESCE(sales_channel, payload->>'sales_channel', 'online') AS sales_channel,
                payload->>'source_row_id' AS source_row_id,
                bronze_file_name, bronze_row_number, bronze_source_record_id, bronze_checksum,
                deterministic_key
            FROM base_order_items
            WHERE order_item_id IS NOT NULL AND TRIM(order_item_id) <> ''
            ORDER BY order_item_id, ingested_at DESC
        )
        INSERT INTO silver.order_items (
            order_item_id, order_id, product_id, promotion_id, quantity,
            unit_price, item_discount_amount, discount_amount, subtotal,
            sales_channel, source_row_id, bronze_file_name, bronze_row_number,
            bronze_source_record_id, bronze_checksum, silver_processed_at,
            is_orphan, deterministic_key
        )
        SELECT 
            order_item_id, order_id, product_id, promotion_id, quantity,
            unit_price, item_discount_amount, discount_amount, subtotal,
            sales_channel, source_row_id, bronze_file_name, bronze_row_number,
            bronze_source_record_id, bronze_checksum, CURRENT_TIMESTAMP,
            FALSE, deterministic_key
        FROM deduplicated_order_items
        ON CONFLICT (order_item_id) DO UPDATE 
        SET 
            order_id = EXCLUDED.order_id, product_id = EXCLUDED.product_id,
            promotion_id = EXCLUDED.promotion_id, quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price, item_discount_amount = EXCLUDED.item_discount_amount,
            discount_amount = EXCLUDED.discount_amount, subtotal = EXCLUDED.subtotal,
            sales_channel = EXCLUDED.sales_channel, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 6C. ORDER_PROMOTIONS
        cur.execute("TRUNCATE silver.order_promotions RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.order_promotions', """
        WITH base_order_promotions AS (
            SELECT 
                order_id, promotion_id, discount_amount, payload,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                ingested_at,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.order_promotions'
              AND order_id IS NOT NULL
              AND promotion_id IS NOT NULL
        ),
        deduplicated_order_promotions AS (
            SELECT DISTINCT ON (order_id, promotion_id)
                order_id, promotion_id,
                COALESCE(discount_amount, (payload->>'discount_amount')::int, 0) AS discount_amount,
                payload->>'source_row_id' AS source_row_id,
                bronze_file_name, bronze_row_number, bronze_source_record_id, bronze_checksum,
                deterministic_key
            FROM base_order_promotions
            ORDER BY order_id, promotion_id, ingested_at DESC
        )
        INSERT INTO silver.order_promotions (
            order_id, promotion_id, discount_amount, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            order_id, promotion_id, discount_amount, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM deduplicated_order_promotions
        ON CONFLICT (order_id, promotion_id) DO UPDATE 
        SET 
            discount_amount = EXCLUDED.discount_amount, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)

       # 7. PAYMENT_EVENTS (DIPERBAIKI DENGAN DISTINCT ON)
        cur.execute("TRUNCATE silver.payment_events RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.payment_events', """
        WITH base_payments AS (
            SELECT 
                COALESCE(payload->>'payment_id', order_id) AS payment_id,
                order_id, event_id, event_type, amount, status AS payment_status, payload,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                ingested_at,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS computed_deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.payment_events'
              AND (payload->>'payment_id' IS NOT NULL OR order_id IS NOT NULL)
        ),
        deduplicated_payments AS (
            SELECT DISTINCT ON (payment_id)
                payment_id,
                order_id,
                COALESCE(event_id, payload->>'event_id') AS event_id,
                COALESCE(event_type, payload->>'event_type', 'payment') AS event_type,
                COALESCE(amount, (payload->>'amount')::numeric, 0) AS amount,
                COALESCE(payment_status, payload->>'payment_status', payload->>'status', 'SUCCESS') AS payment_status,
                payload->>'payment_method' AS payment_method,
                payload->>'ingested_at_utc' AS ingested_at_utc,
                payload->>'occurred_at_utc' AS occurred_at_utc,
                NULLIF(payload->>'occurred_at_utc', '')::TIMESTAMPTZ AS payment_event_time,
                NULLIF(payload->>'occurred_at_utc', '')::TIMESTAMPTZ AS occurred_ts,
                NULLIF(payload->>'ingested_at_utc', '')::TIMESTAMPTZ AS source_ingested_ts,
                payload->>'source_row_id' AS source_row_id,
                bronze_file_name, bronze_row_number, bronze_source_record_id, bronze_checksum,
                computed_deterministic_key AS deterministic_key
            FROM base_payments
            WHERE payment_id IS NOT NULL AND TRIM(payment_id) <> ''
            ORDER BY payment_id, ingested_at DESC
        )
        INSERT INTO silver.payment_events (
            payment_id, order_id, event_id, event_type, amount,
            payment_status, payment_method, ingested_at_utc, occurred_at_utc,
            payment_event_time, occurred_ts, source_ingested_ts, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            payment_id, order_id, event_id, event_type, amount,
            payment_status, payment_method, ingested_at_utc, occurred_at_utc,
            payment_event_time, occurred_ts, source_ingested_ts, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM deduplicated_payments
        ON CONFLICT (payment_id) DO UPDATE 
        SET 
            order_id = EXCLUDED.order_id, event_id = EXCLUDED.event_id,
            event_type = EXCLUDED.event_type, amount = EXCLUDED.amount,
            payment_status = EXCLUDED.payment_status, payment_method = EXCLUDED.payment_method,
            ingested_at_utc = EXCLUDED.ingested_at_utc, occurred_at_utc = EXCLUDED.occurred_at_utc,
            payment_event_time = EXCLUDED.payment_event_time, occurred_ts = EXCLUDED.occurred_ts,
            source_ingested_ts = EXCLUDED.source_ingested_ts, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 8. REFUND_EVENTS
        cur.execute("TRUNCATE silver.refund_events RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.refund_events', """
        WITH base_refunds AS (
            SELECT 
                COALESCE(refund_id, payload->>'refund_id', order_id) AS refund_id,
                order_id, event_id, event_type, amount, payload,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                ingested_at,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS computed_deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.refund_events'
              AND (refund_id IS NOT NULL OR payload->>'refund_id' IS NOT NULL OR order_id IS NOT NULL)
        ),
        deduplicated_refunds AS (
            SELECT DISTINCT ON (refund_id)
                refund_id, order_id,
                COALESCE(event_id, payload->>'event_id') AS event_id,
                COALESCE(event_type, payload->>'event_type', 'refund') AS event_type,
                COALESCE(amount, (payload->>'amount')::numeric, 0) AS amount,
                payload->>'ingested_at_utc' AS ingested_at_utc,
                payload->>'occurred_at_utc' AS occurred_at_utc,
                NULLIF(payload->>'occurred_at_utc', '')::TIMESTAMPTZ AS occurred_ts,
                NULLIF(payload->>'ingested_at_utc', '')::TIMESTAMPTZ AS source_ingested_ts,
                payload->>'source_row_id' AS source_row_id,
                bronze_file_name, bronze_row_number, bronze_source_record_id, bronze_checksum,
                computed_deterministic_key AS deterministic_key
            FROM base_refunds
            WHERE refund_id IS NOT NULL AND TRIM(refund_id) <> ''
            ORDER BY refund_id, ingested_at DESC
        )
        INSERT INTO silver.refund_events (
            refund_id, order_id, event_id, event_type, amount, ingested_at_utc,
            occurred_at_utc, occurred_ts, source_ingested_ts, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            refund_id, order_id, event_id, event_type, amount, ingested_at_utc,
            occurred_at_utc, occurred_ts, source_ingested_ts, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM deduplicated_refunds
        ON CONFLICT (refund_id) DO UPDATE 
        SET 
            order_id = EXCLUDED.order_id, event_id = EXCLUDED.event_id,
            event_type = EXCLUDED.event_type, amount = EXCLUDED.amount,
            ingested_at_utc = EXCLUDED.ingested_at_utc, occurred_at_utc = EXCLUDED.occurred_at_utc,
            occurred_ts = EXCLUDED.occurred_ts, source_ingested_ts = EXCLUDED.source_ingested_ts,
            source_row_id = EXCLUDED.source_row_id, bronze_file_name = EXCLUDED.bronze_file_name,
            bronze_row_number = EXCLUDED.bronze_row_number, bronze_source_record_id = EXCLUDED.bronze_source_record_id,
            bronze_checksum = EXCLUDED.bronze_checksum, silver_processed_at = CURRENT_TIMESTAMP,
            deterministic_key = EXCLUDED.deterministic_key;
        """)

      # 9. RETURN_EVENTS
        cur.execute("TRUNCATE silver.return_events RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.return_events', """
        WITH base_returns AS (
            SELECT 
                COALESCE(return_id, payload->>'return_id', payload->>'id', order_id) AS return_id,
                order_id, event_id, event_type, payload,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                ingested_at,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS computed_deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.return_events'
              AND (return_id IS NOT NULL OR payload->>'return_id' IS NOT NULL OR payload->>'id' IS NOT NULL OR order_id IS NOT NULL)
        ),
        deduplicated_returns AS (
            SELECT DISTINCT ON (return_id)
                return_id, order_id,
                COALESCE(event_id, payload->>'event_id') AS event_id,
                COALESCE(event_type, payload->>'event_type', 'return') AS event_type,
                payload->>'ingested_at_utc' AS ingested_at_utc,
                payload->>'occurred_at_utc' AS occurred_at_utc,
                NULLIF(payload->>'occurred_at_utc', '')::TIMESTAMPTZ AS occurred_ts,
                NULLIF(payload->>'ingested_at_utc', '')::TIMESTAMPTZ AS source_ingested_ts,
                payload->>'source_row_id' AS source_row_id,
                bronze_file_name, bronze_row_number, bronze_source_record_id, bronze_checksum,
                computed_deterministic_key AS deterministic_key
            FROM base_returns
            WHERE return_id IS NOT NULL AND TRIM(return_id) <> ''
            ORDER BY return_id, ingested_at DESC
        )
        INSERT INTO silver.return_events (
            return_id, order_id, event_id, event_type, ingested_at_utc,
            occurred_at_utc, occurred_ts, source_ingested_ts, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            return_id, order_id, event_id, event_type, ingested_at_utc,
            occurred_at_utc, occurred_ts, source_ingested_ts, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM deduplicated_returns
        ON CONFLICT (return_id) DO UPDATE 
        SET 
            order_id = EXCLUDED.order_id, event_id = EXCLUDED.event_id,
            event_type = EXCLUDED.event_type, ingested_at_utc = EXCLUDED.ingested_at_utc,
            occurred_at_utc = EXCLUDED.occurred_at_utc, occurred_ts = EXCLUDED.occurred_ts,
            source_ingested_ts = EXCLUDED.source_ingested_ts, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)
        
        # 10. SUPPORT_EVENTS
        cur.execute("TRUNCATE silver.support_events RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.support_events', """
        WITH base_support AS (
            SELECT 
                COALESCE(payload->>'ticket_id', source_record_id) AS ticket_id,
                customer_id, order_id, reason, event_id, event_type, payload,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                ingested_at,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS computed_deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.support_events'
              AND (payload->>'ticket_id' IS NOT NULL OR source_record_id IS NOT NULL)
        ),
        deduplicated_support AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id, customer_id, order_id,
                COALESCE(reason, payload->>'reason') AS reason,
                COALESCE(event_id, payload->>'event_id') AS event_id,
                COALESCE(event_type, payload->>'event_type', 'support_ticket') AS event_type,
                payload->>'ingested_at_utc' AS ingested_at_utc,
                payload->>'occurred_at_utc' AS occurred_at_utc,
                NULLIF(payload->>'occurred_at_utc', '')::TIMESTAMPTZ AS occurred_ts,
                NULLIF(payload->>'ingested_at_utc', '')::TIMESTAMPTZ AS source_ingested_ts,
                payload->>'source_row_id' AS source_row_id,
                bronze_file_name, bronze_row_number, bronze_source_record_id, bronze_checksum,
                computed_deterministic_key AS deterministic_key
            FROM base_support
            WHERE ticket_id IS NOT NULL AND TRIM(ticket_id) <> ''
            ORDER BY ticket_id, ingested_at DESC
        )
        INSERT INTO silver.support_events (
            ticket_id, customer_id, order_id, reason, event_id, event_type,
            ingested_at_utc, occurred_at_utc, occurred_ts, source_ingested_ts,
            source_row_id, bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            ticket_id, customer_id, order_id, reason, event_id, event_type,
            ingested_at_utc, occurred_at_utc, occurred_ts, source_ingested_ts,
            source_row_id, bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM deduplicated_support
        ON CONFLICT (ticket_id) DO UPDATE 
        SET 
            customer_id = EXCLUDED.customer_id, order_id = EXCLUDED.order_id,
            reason = EXCLUDED.reason, event_id = EXCLUDED.event_id,
            event_type = EXCLUDED.event_type, ingested_at_utc = EXCLUDED.ingested_at_utc,
            occurred_at_utc = EXCLUDED.occurred_at_utc, occurred_ts = EXCLUDED.occurred_ts,
            source_ingested_ts = EXCLUDED.source_ingested_ts, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)

        # 11. WEB_EVENTS
        cur.execute("TRUNCATE silver.web_events RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.web_events', """
        WITH base_web AS (
            SELECT 
                COALESCE(event_id, payload->>'event_id', source_record_id) AS event_id,
                session_id, customer_id, order_id, campaign_id, channel,
                sales_channel, event_type, payload,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                ingested_at,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS computed_deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.web_events'
              AND (event_id IS NOT NULL OR payload->>'event_id' IS NOT NULL OR source_record_id IS NOT NULL)
        ),
        deduplicated_web AS (
            SELECT DISTINCT ON (event_id)
                event_id, session_id, customer_id, order_id, campaign_id,
                COALESCE(channel, payload->>'channel') AS channel,
                COALESCE(sales_channel, payload->>'sales_channel') AS sales_channel,
                COALESCE(event_type, payload->>'event_type', 'page_view') AS event_type,
                payload->>'page_url' AS page_url,
                payload->>'ingested_at_utc' AS ingested_at_utc,
                payload->>'occurred_at_utc' AS occurred_at_utc,
                NULLIF(payload->>'occurred_at_utc', '')::TIMESTAMPTZ AS event_timestamp_utc,
                NULLIF(payload->>'occurred_at_utc', '')::TIMESTAMPTZ AS occurred_ts,
                NULLIF(payload->>'ingested_at_utc', '')::TIMESTAMPTZ AS source_ingested_ts,
                payload->>'source_row_id' AS source_row_id,
                bronze_file_name, bronze_row_number, bronze_source_record_id, bronze_checksum,
                computed_deterministic_key AS deterministic_key
            FROM base_web
            WHERE event_id IS NOT NULL AND TRIM(event_id) <> ''
            ORDER BY event_id, ingested_at DESC
        )
        INSERT INTO silver.web_events (
            event_id, session_id, customer_id, order_id, campaign_id, channel,
            sales_channel, event_type, page_url, ingested_at_utc, occurred_at_utc,
            event_timestamp_utc, occurred_ts, source_ingested_ts, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            event_id, session_id, customer_id, order_id, campaign_id, channel,
            sales_channel, event_type, page_url, ingested_at_utc, occurred_at_utc,
            event_timestamp_utc, occurred_ts, source_ingested_ts, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM deduplicated_web
        ON CONFLICT (event_id) DO UPDATE 
        SET 
            session_id = EXCLUDED.session_id, customer_id = EXCLUDED.customer_id,
            order_id = EXCLUDED.order_id, campaign_id = EXCLUDED.campaign_id,
            channel = EXCLUDED.channel, sales_channel = EXCLUDED.sales_channel,
            event_type = EXCLUDED.event_type, page_url = EXCLUDED.page_url,
            ingested_at_utc = EXCLUDED.ingested_at_utc, occurred_at_utc = EXCLUDED.occurred_at_utc,
            event_timestamp_utc = EXCLUDED.event_timestamp_utc, occurred_ts = EXCLUDED.occurred_ts,
            source_ingested_ts = EXCLUDED.source_ingested_ts, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key;
        """)

      # 12. INVENTORY_SNAPSHOTS
        cur.execute("TRUNCATE silver.inventory_snapshots RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.inventory_snapshots', """
        WITH base_inventory AS (
            SELECT 
                COALESCE(source_record_id, product_id || '|' || store_id || '|' || COALESCE(snapshot_date, current_date::text)) AS inventory_id,
                product_id, store_id, stock_on_hand, stock_reserved, snapshot_date, payload,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                ingested_at,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS computed_deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.inventory_snapshots'
              AND (product_id IS NOT NULL OR store_id IS NOT NULL)
        ),
        deduplicated_inventory AS (
            SELECT DISTINCT ON (inventory_id, snapshot_date_utc)
                inventory_id, product_id, store_id,
                COALESCE(stock_on_hand, (payload->>'stock_quantity')::int, (payload->>'stock_on_hand')::int, 0) AS stock_quantity,
                COALESCE(stock_on_hand, (payload->>'stock_on_hand')::int, 0) AS stock_on_hand,
                COALESCE(stock_reserved, (payload->>'stock_reserved')::int, 0) AS stock_reserved,
                COALESCE(NULLIF(snapshot_date, '')::date, current_date) AS snapshot_date,
                COALESCE(NULLIF(snapshot_date, '')::date, current_date) AS snapshot_date_utc,
                payload->>'source_row_id' AS source_row_id,
                bronze_file_name, bronze_row_number, bronze_source_record_id, bronze_checksum,
                computed_deterministic_key AS deterministic_key,
                NULLIF(payload->>'snapshot_timestamp_utc', '')::TIMESTAMPTZ AS snapshot_timestamp_utc
            FROM base_inventory
            WHERE inventory_id IS NOT NULL AND TRIM(inventory_id) <> ''
            ORDER BY inventory_id, snapshot_date_utc, ingested_at DESC
        )
        INSERT INTO silver.inventory_snapshots (
            inventory_id, product_id, store_id, stock_quantity, stock_on_hand,
            stock_reserved, snapshot_date, snapshot_date_utc, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, deterministic_key,
            snapshot_timestamp_utc
        )
        SELECT 
            inventory_id, product_id, store_id, stock_quantity, stock_on_hand,
            stock_reserved, snapshot_date, snapshot_date_utc, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, deterministic_key,
            snapshot_timestamp_utc
        FROM deduplicated_inventory
        ON CONFLICT (inventory_id, snapshot_date_utc) DO UPDATE 
        SET 
            product_id = EXCLUDED.product_id, store_id = EXCLUDED.store_id,
            stock_quantity = EXCLUDED.stock_quantity, stock_on_hand = EXCLUDED.stock_on_hand,
            stock_reserved = EXCLUDED.stock_reserved, source_row_id = EXCLUDED.source_row_id,
            bronze_file_name = EXCLUDED.bronze_file_name, bronze_row_number = EXCLUDED.bronze_row_number,
            bronze_source_record_id = EXCLUDED.bronze_source_record_id, bronze_checksum = EXCLUDED.bronze_checksum,
            silver_processed_at = CURRENT_TIMESTAMP, deterministic_key = EXCLUDED.deterministic_key,
            snapshot_timestamp_utc = EXCLUDED.snapshot_timestamp_utc;
        """)

        # 13. CAMPAIGN_SPEND
        cur.execute("TRUNCATE silver.campaign_spend RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.campaign_spend', """
        WITH base_spend AS (
            SELECT 
                COALESCE(campaign_id, source_record_id) AS campaign_id,
                spend_date, channel_id, spend_amount, payload,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                ingested_at,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS computed_deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.campaign_spend'
              AND (campaign_id IS NOT NULL OR source_record_id IS NOT NULL)
        ),
        deduplicated_spend AS (
            SELECT DISTINCT ON (campaign_id, spend_date_utc)
                campaign_id,
                COALESCE(spend_date, (payload->>'spend_date')::date, current_date) AS spend_date_utc,
                COALESCE(channel_id, payload->>'channel_id') AS channel_id,
                COALESCE(spend_amount, (payload->>'spend_amount')::numeric, 0) AS spend_amount,
                payload->>'source_row_id' AS source_row_id,
                bronze_file_name, bronze_row_number, bronze_source_record_id, bronze_checksum,
                computed_deterministic_key AS deterministic_key,
                NULLIF(payload->>'snapshot_timestamp_utc', '')::TIMESTAMPTZ AS snapshot_timestamp_utc
            FROM base_spend
            WHERE campaign_id IS NOT NULL AND TRIM(campaign_id) <> ''
            ORDER BY campaign_id, spend_date_utc, ingested_at DESC
        )
        INSERT INTO silver.campaign_spend (
            campaign_id, spend_date_utc, channel_id, spend_amount, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, deterministic_key,
            snapshot_timestamp_utc
        )
        SELECT 
            campaign_id, spend_date_utc, channel_id, spend_amount, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, deterministic_key,
            snapshot_timestamp_utc
        FROM deduplicated_spend
        ON CONFLICT (campaign_id, spend_date_utc) DO UPDATE 
        SET 
            channel_id = EXCLUDED.channel_id, spend_amount = EXCLUDED.spend_amount,
            source_row_id = EXCLUDED.source_row_id, bronze_file_name = EXCLUDED.bronze_file_name,
            bronze_row_number = EXCLUDED.bronze_row_number, bronze_source_record_id = EXCLUDED.bronze_source_record_id,
            bronze_checksum = EXCLUDED.bronze_checksum, silver_processed_at = CURRENT_TIMESTAMP,
            deterministic_key = EXCLUDED.deterministic_key, snapshot_timestamp_utc = EXCLUDED.snapshot_timestamp_utc;
        """)

       # 14. PROMOTIONS
        cur.execute("TRUNCATE silver.promotions RESTART IDENTITY CASCADE;")
        conn.commit()
        safe_insert('silver.promotions', """
        WITH base_promotions AS (
            SELECT 
                COALESCE(promotion_id, payload->>'promotion_id', payload #>> '{payload,promotion_id}', payload->>'id') AS promotion_id,
                COALESCE(promotion_code, payload->>'promotion_code', payload #>> '{payload,promotion_code}') AS promotion_code,
                COALESCE(promotion_type, payload->>'promotion_type', payload #>> '{payload,promotion_type}') AS promotion_type,
                COALESCE(discount_rate, NULLIF(payload->>'discount_rate', '')::NUMERIC, NULLIF(payload #>> '{payload,discount_rate}', '')::NUMERIC, 0.00) AS discount_rate,
                COALESCE(discount_amount::numeric, NULLIF(payload->>'discount_amount', '')::NUMERIC, NULLIF(payload #>> '{payload,discount_amount}', '')::NUMERIC, 0.00) AS discount_amount,
                COALESCE(start_date, NULLIF(payload->>'start_date', '')::DATE, NULLIF(payload #>> '{payload,start_date}', '')::DATE) AS start_date,
                COALESCE(end_date, NULLIF(payload->>'end_date', '')::DATE, NULLIF(payload #>> '{payload,end_date}', '')::DATE) AS end_date,
                payload->>'source_row_id' AS source_row_id,
                file_name AS bronze_file_name, row_number AS bronze_row_number,
                source_record_id AS bronze_source_record_id, checksum AS bronze_checksum,
                ingested_at,
                md5(
                    COALESCE(target_table, '') || '|' ||
                    COALESCE(source_record_id, bronze_id::text) || '|' ||
                    COALESCE(checksum, '') || '|' ||
                    COALESCE(row_number::text, '0')
                ) AS computed_deterministic_key
            FROM bronze.raw_data
            WHERE processing_status = 'NEW'
              AND target_table = 'silver.promotions'
        ),
        deduplicated_promotions AS (
            SELECT DISTINCT ON (promotion_id)
                promotion_id, promotion_code, promotion_type, discount_rate,
                discount_amount, start_date, end_date, source_row_id,
                bronze_file_name, bronze_row_number, bronze_source_record_id,
                bronze_checksum, computed_deterministic_key AS deterministic_key
            FROM base_promotions
            WHERE promotion_id IS NOT NULL AND TRIM(promotion_id) <> ''
            ORDER BY promotion_id, ingested_at DESC
        )
        INSERT INTO silver.promotions (
            promotion_id, promotion_code, promotion_type, discount_rate,
            discount_amount, start_date, end_date, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, silver_processed_at, is_current, deterministic_key
        )
        SELECT 
            promotion_id, promotion_code, promotion_type, discount_rate,
            discount_amount, start_date, end_date, source_row_id,
            bronze_file_name, bronze_row_number, bronze_source_record_id,
            bronze_checksum, CURRENT_TIMESTAMP, TRUE, deterministic_key
        FROM deduplicated_promotions
        ON CONFLICT (promotion_id) DO UPDATE 
        SET 
            promotion_code = EXCLUDED.promotion_code, promotion_type = EXCLUDED.promotion_type,
            discount_rate = EXCLUDED.discount_rate, discount_amount = EXCLUDED.discount_amount,
            start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date,
            source_row_id = EXCLUDED.source_row_id, bronze_file_name = EXCLUDED.bronze_file_name,
            bronze_row_number = EXCLUDED.bronze_row_number, bronze_source_record_id = EXCLUDED.bronze_source_record_id,
            bronze_checksum = EXCLUDED.bronze_checksum, silver_processed_at = CURRENT_TIMESTAMP,
            deterministic_key = EXCLUDED.deterministic_key;
        """)

        # FINAL CHECK
        print("\n=== FINAL SILVER CHECK ===")
        tables = [
            'silver.customers', 'silver.stores', 'silver.products',
            'silver.product_categories', 'silver.orders', 'silver.order_items',
            'silver.payment_events', 'silver.order_promotions', 'silver.campaign_spend',
            'silver.city_reference', 'silver.sales_channels', 'silver.customer_addresses',
            'silver.refund_events', 'silver.return_events', 'silver.support_events', 'silver.promotions',
            'silver.web_events', 'silver.inventory_snapshots', 'silver.customer_profiles'
        ]
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                cnt = cur.fetchone()[0]
                print(f"{'✅' if cnt > 0 else '🚨'} {t:35} | {cnt}")
            except Exception as e:
                print(f"⚠️ {t:35} | Gagal query ({e})")
                conn.rollback()

        cur.execute("SET session_replication_role = origin;")
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"FAIL stage2: {e}")
        traceback.print_exc()
        raise
    finally:
        try:
            cur.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass