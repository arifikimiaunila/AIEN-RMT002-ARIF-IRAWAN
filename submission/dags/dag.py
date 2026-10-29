from datetime import datetime
import importlib.util
import os
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2

AIRFLOW_DAGS = Path(__file__).resolve().parent

def find_project_root():
    for c in [Path("/opt/airflow"), AIRFLOW_DAGS.parent, Path.cwd(), Path("/opt/airflow/dags").parent, AIRFLOW_DAGS]:
        if (c / "database" / "schemas").exists():
            return c
        if (c / "database").exists():
            return c
        if (c / "raw_data_validation").exists():
            return c
    return Path("/opt/airflow")

BASE_DIR = find_project_root()

def find_schemas_dir():
    candidates = [
        Path("/opt/airflow/database/schemas"),
        Path("/opt/airflow/dags/database/schemas"),
        BASE_DIR / "database" / "schemas",
        BASE_DIR / "dags" / "database" / "schemas",
        Path.cwd() / "database" / "schemas",
        AIRFLOW_DAGS / "database" / "schemas",
        AIRFLOW_DAGS.parent / "database" / "schemas",
    ]
    for p in candidates:
        if p.exists() and list(p.glob("*.sql")):
            return p
    for root in [Path("/opt/airflow"), BASE_DIR, Path.cwd()]:
        if not root.exists(): continue
        for p in root.rglob("bronze.sql"):
            if p.parent.exists():
                return p.parent
    return Path("/opt/airflow/database/schemas")

SCHEMAS_DIR = find_schemas_dir()
INCLUDE_DIR = BASE_DIR / "include"
RAW_VALID_DIR = BASE_DIR / "raw_data_validation"
PIPELINES_DIR = BASE_DIR / "pipelines"

if not INCLUDE_DIR.exists(): INCLUDE_DIR = Path("/opt/airflow/include")
if not RAW_VALID_DIR.exists(): RAW_VALID_DIR = Path("/opt/airflow/raw_data_validation")
if not PIPELINES_DIR.exists(): PIPELINES_DIR = Path("/opt/airflow/pipelines")

sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(RAW_VALID_DIR))

def find_file(name: str) -> Path:
    for p in [
        RAW_VALID_DIR / name,
        INCLUDE_DIR / name,
        PIPELINES_DIR / name,
        SCHEMAS_DIR / name,
        Path("/opt/airflow/raw_data_validation") / name,
        Path("/opt/airflow/include") / name,
        Path("/opt/airflow/pipelines") / name,
        Path("/opt/airflow/database/schemas") / name,
        BASE_DIR / "raw_data_validation" / name,
        BASE_DIR / "pipelines" / name,
    ]:
        if p.exists():
            return p
    return RAW_VALID_DIR / name

def find_file_recursive(name: str) -> Path:
    for root in [PIPELINES_DIR, Path("/opt/airflow/pipelines"), BASE_DIR / "pipelines", RAW_VALID_DIR, Path("/opt/airflow/raw_data_validation"), INCLUDE_DIR]:
        if not root.exists(): continue
        for p in root.rglob(name):
            if p.is_file():
                return p
    return find_file(name)

def get_conn():
    from airflow.hooks.base import BaseHook
    try:
        c = BaseHook.get_connection("retail_postgres")
        return psycopg2.connect(
            host=c.host or "postgres", 
            port=c.port or 5432, 
            dbname=c.schema or "retail_analytics", 
            user=c.login or "retail", 
            password=c.password or "retail"
        )
    except Exception:
        return psycopg2.connect(
            host="postgres", 
            port=5432, 
            dbname="retail_analytics", 
            user="retail", 
            password="retail"
        )

def run_sql_file(file_name: str):
    p = None
    for root in [SCHEMAS_DIR, Path("/opt/airflow/database/schemas"), BASE_DIR / "database" / "schemas"]:
        if not root.exists(): continue
        direct = root / file_name
        if direct.exists():
            p = direct
            break
        for found in root.rglob(file_name):
            p = found
            break
        if p: break
    if not p or not p.exists():
        raise FileNotFoundError(f"{file_name} not found")
    sql = p.read_text()
    print(f"[SQL] Running {p}")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    print(f"[SQL] SUCCESS {file_name}")
    cur.close()
    conn.close()

def _load_and_run(module_name, file_path, func_name="main", **context):
    file_path = Path(file_path)
    if not file_path.exists():
        file_path = find_file_recursive(file_path.name)
    print(f"[DAG] Loading {module_name} from {file_path}")
    
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    
    # Suntikkan pipeline_run_id dari Airflow ke context agar dapat dilacak di setiap layer
    run_id = context.get("run_id", "manual_run")
    dag_id = context.get("dag", getattr(context.get("task", None), "dag_id", "omnichannel_retail_pipeline"))
    
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    
    # Catat status mulai ke ops.pipeline_runs secara idempoten / insert baru
    try:
        cur.execute(
            "INSERT INTO ops.pipeline_runs (run_id, dag_id, status, started_at) VALUES (%s, %s, %s, CURRENT_TIMESTAMP);",
            (run_id, dag_id, "RUNNING")
        )
    except Exception as e:
        print(f"[OPS LOG] Warning tracking pipeline run: {e}")
    finally:
        cur.close()
        conn.close()

    try:
        for n in [func_name, "main", "build_bronze_dag_callable", "build_silver_dag_callable", "build_gold_dag_callable", "build_bronze", "build_silver", "build_gold"]:
            if hasattr(mod, n):
                print(f"[DAG] Found callable {n} with run_id={run_id}")
                res = getattr(mod, n)(**context)
                
                # Catat status SUKSES ke ops
                conn = get_conn()
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(
                    "UPDATE ops.pipeline_runs SET status = 'SUCCESS', finished_at = CURRENT_TIMESTAMP WHERE run_id = %s;",
                    (run_id,)
                )
                cur.close()
                conn.close()
                return res
                
        raise AttributeError(f"{func_name} not found in {file_path}")
        
    except Exception as err:
        # Catat status GAGAL dan error pada schema ops
        conn = get_conn()
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE ops.pipeline_runs SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP WHERE run_id = %s;",
                (run_id,)
            )
            cur.execute(
                "INSERT INTO ops.silver_data_quality_events (check_name, target_table, severity, issue_type, description) VALUES (%s, %s, %s, %s, %s);",
                ("PIPELINE_EXECUTION_ERROR", module_name, "critical", "EXCEPTION", str(err))
            )
        except Exception as log_err:
            print(f"[OPS LOG] Error writing failure log: {log_err}")
        finally:
            cur.close()
            conn.close()
        raise err

with DAG(
    dag_id="omnichannel_retail_pipeline", 
    start_date=datetime(2026, 10, 19), 
    schedule=None, 
    catchup=False, 
    tags=["medallion", "local_postgres"]
) as dag:

    def validate(**context):
        return _load_and_run("raw_data_validation", find_file("raw_data_validation.py"), "validate_raw_data", **context)

    def build_bronze(**context):
        return _load_and_run("build_bronze", find_file_recursive("build_bronze.py"), **context)

    def build_silver(**context):
        return _load_and_run("build_silver", find_file_recursive("build_silver.py"), **context)

    def build_gold(**context):
        return _load_and_run("build_gold", find_file_recursive("build_gold.py"), **context)

    validate_raw_data = PythonOperator(task_id="validate_raw_data", python_callable=validate)
    
    create_ops_schema = PythonOperator(task_id="create_ops_schema", python_callable=lambda: run_sql_file("ops.sql"))
    create_bronze_schema = PythonOperator(task_id="create_bronze_schema", python_callable=lambda: run_sql_file("bronze.sql"))
    create_silver_schema = PythonOperator(task_id="create_silver_schema", python_callable=lambda: run_sql_file("silver.sql"))
    create_gold_schema = PythonOperator(task_id="create_gold_schema", python_callable=lambda: run_sql_file("gold.sql"))

    build_bronze_layer = PythonOperator(task_id="build_bronze_layer", python_callable=build_bronze)
    build_silver_layer = PythonOperator(task_id="build_silver_layer", python_callable=build_silver)
    build_gold_layer = PythonOperator(task_id="build_gold_layer", python_callable=build_gold)

    # Alur eksekusi DAG terstruktur:
    # 1. Validasi Raw Data -> 2. Buat Schema Operasional & Medallion -> 3. Bronze -> 4. Silver (termasuk Quality Gate & Blokir) -> 5. Gold
    create_ops_schema >> validate_raw_data >> [create_bronze_schema, create_silver_schema, create_gold_schema] >> build_bronze_layer >> build_silver_layer >> build_gold_layer
