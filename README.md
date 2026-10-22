markdown# Omnichannel Retail - Medallion Pipeline

Pipeline: raw JSON/CSV → Bronze → Silver → Gold → Ops
Target: PostgreSQL lokal `retail-postgres:5432` (retail_analytics)
Airflow: `retail-airflow:8080` admin/admin

## Struktur Submission
submission/
├── dags/
│   └── dag.py                          # DAG omnichannel_retail_pipeline
├── database/schemas/
│   ├── bronze.sql
│   ├── silver.sql
│   ├── gold.sql
│   └── ops.sql
├── pipelines/
│   ├── bronze/build_bronze.py
│   ├── silver/build_silver.py
│   └── gold/build_gold.py
└── evidence/
    ├── airflow_run.png
    └── sample_engine_queries.mdjavascript
## Prinsip

- Idempotent: rerun tanpa duplikasi data
- `pipeline_run_id` dikirim ke setiap layer via XCom
- Quality gate menghentikan tahap berikutnya jika FAIL
- Status dan error disimpan di schema `ops`
- PostgreSQL lokal sebagai target, tidak upload Bronze/Silver ke Neon

## Services

- `postgres:16` - retail-postgres:5432 - retail_analytics
- `marquez-api:5000` + `marquez-web:3000`
- `nl2sql-api:8000`
- `analytics-ui:8501`
- `airflow:2.9.3` - 8080 - volumes dags, database/schemas, pipelines, raw_data_validation

## Cara Menjalankan
### 1. Unduh docker_compose.yaml.
### 2. Start stack
```bash
docker-compose up -d --build
docker-compose ps2. Trigger DAGbash# UI: http://localhost:8080 admin/admin → Trigger omnichannel_retail_pipeline
docker exec -i retail-airflow airflow dags trigger omnichannel_retail_pipeline3. Cek Goldbashdocker exec -i retail-postgres psql -U retail -d retail_analytics -c "
SELECT 'gold.order_360' as tbl, COUNT(*) FROM gold.order_360
UNION ALL SELECT 'gold.customer_daily', COUNT(*) FROM gold.customer_daily
UNION ALL SELECT 'gold.product_daily', COUNT(*) FROM gold.product_daily;
"4. Cek Metadatabashdocker exec -i retail-postgres psql -U retail -d retail_analytics -c "
SELECT run_id, status, started_at FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 3;
SELECT check_name, status, details FROM ops.data_quality_log ORDER BY checked_at DESC LIMIT 10;
"Cara Uji File Query Submission
File query ada di evidence/sample_engine_queries.md
Via psqlbashdocker cp evidence/sample_engine_queries.md retail-postgres:/tmp/
docker exec -i retail-postgres psql -U retail -d retail_analytics -f /tmp/sample_engine_queries.mdVia nl2sql-apibashcurl -X POST http://localhost:8000/query -H "Content-Type: application/json" -d '{"sql": "SELECT * FROM gold.executive_kpis_daily"}'Via analytics-ui
Buka http://localhost:8501 → ENGINE_URL http://nl2sql-api:8000 → Run query dari evidence
Resetbashdocker-compose down -v
docker-compose up -d --buildEvidence
