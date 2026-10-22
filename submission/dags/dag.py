import sys
import uuid
from pathlib import Path
import importlib.util
from datetime import datetime
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

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
    """PostgreSQL LOKAL ONLY - Tidak ke Neon"""
    from airflow.hooks.base import BaseHook
    try:
        c = BaseHook.get_connection("retail_postgres")
        # FORCE local postgres container, block Neon
        if c.host and "neon" in c.host.lower():
            raise ValueError("BLOCKED: Bronze/Silver tidak boleh upload ke Neon. Gunakan postgres lokal.")
        return psycopg2.connect(
            host=c.host or "postgres", 
            port=c.port or 5432, 
            dbname=c.schema or "retail_analytics", 
            user=c.login or "retail", 
            password=c.password or "retail"
        )
    except Exception as e:
        if "Neon" in str(e) or "BLOCKED" in str(e):
            raise
        return psycopg2.connect(host="postgres", port=5432, dbname="retail_analytics", user="retail", password="retail")

def init_pipeline_run(**context):
    """Generate pipeline_run_id unik & simpan ke ops.pipeline_runs"""
    run_id = f"omnichannel_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    print(f"[INIT] pipeline_run_id={run_id}")
    
    # Simpan ke XCom agar bisa dipakai semua layer
    ti = context['ti']
    ti.xcom_push(key='pipeline_run_id', value=run_id)
    
    # Record START di ops.pipeline_runs
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
                id SERIAL PRIMARY KEY,
                run_id VARCHAR(255) UNIQUE,
                dag_id VARCHAR(255),
                status VARCHAR(50),
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP,
                error_message TEXT
            );
            CREATE TABLE IF NOT EXISTS ops.data_quality_log (
                id SERIAL PRIMARY KEY,
                check_name VARCHAR(255),
                table_name VARCHAR(255),
                status VARCHAR(50),
                details TEXT,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            INSERT INTO ops.pipeline_runs (run_id, dag_id, status, started_at)
            VALUES (%s, %s, 'RUNNING', NOW())
            ON CONFLICT (run_id) DO UPDATE SET status='RUNNING', started_at=NOW();
        """, (run_id, 'omnichannel_retail_pipeline'))
        print(f"[INIT] Recorded RUNNING {run_id} to ops.pipeline_runs")
    finally:
        cur.close()
        conn.close()
    
    return run_id

def _load_and_run(module_name, file_path, func_name="main", **context):
    """Load module + inject pipeline_run_id (idempotent)"""
    file_path = Path(file_path)
    if not file_path.exists():
        file_path = find_file_recursive(file_path.name)
    
    # Ambil pipeline_run_id dari XCom
    ti = context.get('ti')
    pipeline_run_id = None
    if ti:
        pipeline_run_id = ti.xcom_pull(key='pipeline_run_id', task_ids='init_pipeline_run')
    if not pipeline_run_id:
        pipeline_run_id = context.get('pipeline_run_id') or f"omnichannel_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"[DAG] Loading {module_name} from {file_path} with run_id={pipeline_run_id}")
    context['pipeline_run_id'] = pipeline_run_id
    context['run_id'] = pipeline_run_id  # alias untuk build_gold
    
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    
    for n in [func_name, "main", "build_bronze_dag_callable", "build_silver_dag_callable", "build_gold_dag_callable", "build_bronze", "build_silver", "build_gold", "validate_raw_data"]:
        if hasattr(mod, n):
            print(f"[DAG] Found callable {n} with pipeline_run_id={pipeline_run_id}")
            return getattr(mod, n)(**context)
    raise AttributeError(f"{func_name} not in {file_path}")

def run_sql_file(file_name: str, **context):
    """Idempotent - CREATE TABLE IF NOT EXISTS"""
    ti = context.get('ti')
    pipeline_run_id = ti.xcom_pull(key='pipeline_run_id', task_ids='init_pipeline_run') if ti else None
    
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
        raise FileNotFoundError(f"{file_name} not found in {SCHEMAS_DIR}")
    
    sql = p.read_text()
    print(f"[SQL] Running {p} with run_id={pipeline_run_id} (IDEMPOTENT)")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql)
        print(f"[SQL] SUCCESS {file_name} - no duplication (IF NOT EXISTS)")
    finally:
        cur.close()
        conn.close()

def quality_gate_silver(**context):
    """Quality Gate Silver -> Gold: STOP jika FAIL"""
    print("\n=== QUALITY GATE SILVER ===")
    try:
        file_path = find_file_recursive("build_silver.py")
        result = _load_and_run("build_silver_qg", file_path, "stage11_quality_gate", **context)
        print(f"Quality Gate Silver PASS: {result}")
        return result
    except Exception as e:
        # Simpan FAIL ke ops
        ti = context.get('ti')
        run_id = ti.xcom_pull(key='pipeline_run_id', task_ids='init_pipeline_run') if ti else 'unknown'
        conn = get_conn()
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO ops.data_quality_log (check_name, table_name, status, details) VALUES (%s,%s,%s,%s)",
                        ('quality_gate_silver', 'silver->gold', 'FAIL', str(e)[:1000]))
            cur.execute("UPDATE ops.pipeline_runs SET status='FAILED', finished_at=NOW(), error_message=%s WHERE run_id=%s",
                        (str(e)[:1000], run_id))
        finally:
            cur.close()
            conn.close()
        raise AirflowFailException(f"QUALITY GATE SILVER FAIL - STOP PIPELINE: {e}")

def quality_gate_gold(**context):
    """Validate Gold: STOP jika FAIL"""
    print("\n=== QUALITY GATE GOLD ===")
    try:
        file_path = find_file_recursive("build_gold.py")
        result = _load_and_run("build_gold_qg", file_path, "validate_gold", **context)
        print(f"Validate Gold PASS: {result}")
        return result
    except Exception as e:
        ti = context.get('ti')
        run_id = ti.xcom_pull(key='pipeline_run_id', task_ids='init_pipeline_run') if ti else 'unknown'
        conn = get_conn()
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO ops.data_quality_log (check_name, table_name, status, details) VALUES (%s,%s,%s,%s)",
                        ('validate_gold', 'gold', 'FAIL', str(e)[:1000]))
            cur.execute("UPDATE ops.pipeline_runs SET status='FAILED', finished_at=NOW(), error_message=%s WHERE run_id=%s",
                        (str(e)[:1000], run_id))
        finally:
            cur.close()
            conn.close()
        raise AirflowFailException(f"VALIDATE GOLD FAIL - GOLD NOT PUBLISHED: {e}")

def finalize_pipeline(**context):
    """Record SUCCESS ke ops.pipeline_runs"""
    ti = context.get('ti')
    run_id = ti.xcom_pull(key='pipeline_run_id', task_ids='init_pipeline_run') if ti else 'unknown'
    print(f"[FINALIZE] SUCCESS {run_id}")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("UPDATE ops.pipeline_runs SET status='SUCCESS', finished_at=NOW() WHERE run_id=%s", (run_id,))
        # Hitung Gold final
        cur.execute("SELECT COUNT(*) FROM gold.order_360;")
        cnt = cur.fetchone()[0] if cur.rowcount else 0
        cur.execute("INSERT INTO ops.data_quality_log (check_name, table_name, status, details) VALUES (%s,%s,%s,%s)",
                    ('pipeline_finalize', 'omnichannel_retail_pipeline', 'SUCCESS', f'run_id={run_id} gold.order_360={cnt} rows - READY FOR BI'))
        print(f"[FINALIZE] Pipeline {run_id} SUCCESS - {cnt} rows in gold")
    finally:
        cur.close()
        conn.close()
    return run_id

# ================= DAG DEFINITION =================
with DAG(
    dag_id="omnichannel_retail_pipeline",
    start_date=datetime(2026,10,19),
    schedule=None,
    catchup=False,
    tags=["medallion", "local-postgres", "idempotent"],
    doc_md="""
    ## Omnichannel Retail Medallion Pipeline
    - Idempotent: rerun tanpa duplikasi (ON CONFLICT + TRUNCATE + IF NOT EXISTS)
    - pipeline_run_id dikirim ke setiap layer via XCom
    - Quality Gate: stop jika FAIL
    - Status disimpan di ops.pipeline_runs & ops.data_quality_log
    - Target: PostgreSQL lokal postgres:5432 retail_analytics ONLY, tidak upload Bronze/Silver ke Neon
    """
) as dag:

    def validate(**context):
        return _load_and_run("raw_data_validation", find_file("raw_data_validation.py"), "validate_raw_data", **context)

    def build_bronze(**context):
        return _load_and_run("build_bronze", find_file_recursive("build_bronze.py"), **context)

    def build_silver(**context):
        return _load_and_run("build_silver", find_file_recursive("build_silver.py"), **context)

    def build_gold(**context):
        return _load_and_run("build_gold", find_file_recursive("build_gold.py"), **context)

    # Tasks
    init_task = PythonOperator(task_id="init_pipeline_run", python_callable=init_pipeline_run)
    
    validate_raw_data = PythonOperator(task_id="validate_raw_data", python_callable=validate)
    
    create_bronze_schema = PythonOperator(task_id="create_bronze_schema", python_callable=run_sql_file, op_kwargs={"file_name": "bronze.sql"})
    create_silver_schema = PythonOperator(task_id="create_silver_schema", python_callable=run_sql_file, op_kwargs={"file_name": "silver.sql"})
    create_gold_schema = PythonOperator(task_id="create_gold_schema", python_callable=run_sql_file, op_kwargs={"file_name": "gold.sql"})
    create_ops_schema = PythonOperator(task_id="create_ops_schema", python_callable=run_sql_file, op_kwargs={"file_name": "ops.sql"})

    build_bronze_layer = PythonOperator(task_id="build_bronze_layer", python_callable=build_bronze)
    build_silver_layer = PythonOperator(task_id="build_silver_layer", python_callable=build_silver)
    
    # Quality Gate - akan FAIL DAG jika tidak memenuhi syarat Gold
    gate_silver = PythonOperator(task_id="quality_gate_silver", python_callable=quality_gate_silver)
    
    build_gold_layer = PythonOperator(task_id="build_gold_layer", python_callable=build_gold)
    
    gate_gold = PythonOperator(task_id="validate_gold", python_callable=quality_gate_gold)
    
    finalize = PythonOperator(task_id="finalize_pipeline", python_callable=finalize_pipeline)

    # Dependency dengan Quality Gate yang menghentikan next stage
    init_task >> validate_raw_data >> [create_bronze_schema, create_silver_schema, create_gold_schema, create_ops_schema] >> build_bronze_layer >> build_silver_layer >> gate_silver >> build_gold_layer >> gate_gold >> finalize
