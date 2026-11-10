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
### Windows(Powershell)
1. Install Git + Python + Docker Desktop kalau belum ada
winget install Git.Git
winget install Python.Python.3.12
winget install Docker.DockerDesktop
Tutup PowerShell, buka lagi yang baru, lalu buka Docker Desktop
Tunggu sampai Docker Desktop status-nya Running
2. Clone project
git clone https://github.com/arifikimiaunila/AIEN-RMT002-ARIF-IRAWAN.git
cd AIEN-RMT002-ARIF-IRAWAN
3. Bikin venv pakai Python 3.12
py -3.12 -m venv venv
4. Izinkan script venv jalan (cuma sekali, kalau error)
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
5. Aktivasi venv - versi Windows
.\venv\Scripts\Activate.ps1
6. Install dependencies
pip install -r requirements.txt
7. Jalankan Docker, tanpa sudo
docker compose up -d
8. Cek nama container webserver dulu
docker ps
9. Buat user admin Airflow
Di PowerShell, perintahnya harus 1 baris, jangan pakai \ di ujung
docker exec -it <nama_container_airflow_webserver> airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password adminpassword
10. Buka browser dan akses:
URL: http://localhost:8080
Username default: admin
Password default: adminpassword

### MacOS
1. Install Homebrew kalau belum ada
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
2. Install Git + Python + Docker Desktop
brew install git python@3.12
brew install --cask docker
3. Buka Docker Desktop dulu, tunggu sampai icon-nya gak muter lagi
open -a Docker
4. Clone project
git clone https://github.com/arifikimiaunila/AIEN-RMT002-ARIF-IRAWAN.git
cd AIEN-RMT002-ARIF-IRAWAN
5. Bikin venv + aktifin
python3.12 -m venv venv
source venv/bin/activate
6. Install dependencies
pip install -r requirements.txt
7. Jalankan Docker, tanpa sudo
docker compose up -d
8. Cek nama container webserver dulu
docker ps
9. Buat user admin Airflow
docker exec -it <nama_container_airflow_webserver> airflow users create \
--username admin \
--firstname Admin \
--lastname User \
--role Admin \
--email admin@example.com \
--password adminpassword
10. Buka browser dan akses:
URL: http://localhost:8080
Username default: admin
Password default: adminpassword
