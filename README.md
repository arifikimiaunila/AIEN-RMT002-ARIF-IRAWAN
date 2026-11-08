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
- `nl2sql-api:8000`
- `analytics-ui:8501`
- `airflow:2.9.3` - 8080 - volumes dags, database/schemas, pipelines, raw_data_validation

## Cara Menjalankan
### Linux Ubuntu
1. sudo git clone https://github.com/arifikimiaunila/AIEN-RMT002-ARIF-IRAWAN.git.
2. cd AIEN-RMT002-ARIF-IRAWAN
3. sudo apt update
sudo apt install -y python3-venv python3-pip
4. python3 -m venv venv
5. source venv/bin/activate
6. pip install -r requirements.txt
7. sudo docker compose up -d
8. sudo docker exec -it <nama_container_airflow_webserver> airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password adminpassword
9. Buka browser dan akses:
URL: http://localhost:8080
Username default: admin
Password default: adminpassword
