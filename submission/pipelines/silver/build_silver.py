import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
from datetime import datetime, timezone
import hashlib
import json
import os
import pathlib
import traceback
import uuid

from airflow.hooks.base import BaseHook
import pandas as pd
pyspark_installed = True
try:
    from pyspark.sql import SparkSession
except ImportError:
    pyspark_installed = False

import psycopg2

# Impor tahapan pipeline Silver

from silver_files.stage1_lineage import stage1_lineage
from silver_files.stage2_fill_silver import stage2_fill_silver
from silver_files.count_silver_rows import count_silver_rows
from silver_files.stage3_timestamp import stage3_timestamp
from silver_files.stage4_normalization import stage4_normalization
from silver_files.stage5_rejection import stage5_rejection
from silver_files.stage6_dedup import stage6_dedup
from silver_files.stage7_identity import stage7_identity
from silver_files.stage8_temporal import stage8_temporal
from silver_files.stage9_fk import stage9_fk
from silver_files.stage10_late import stage10_late
from silver_files.stage11_quality_gate import stage11_quality_gate 

# Konfigurasi Environment Java & Spark
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def get_pg_conf():
    try:
        c = BaseHook.get_connection("retail_postgres")
        return dict(
            dbname=c.schema or "retail_analytics", 
            user=c.login or "retail", 
            password=c.password or "retail", 
            host=c.host or "postgres", 
            port=c.port or 5432
        )
    except Exception:
        return dict(
            dbname="retail_analytics", 
            user="retail", 
            password="retail", 
            host="postgres", 
            port=5432
        )

# Inisialisasi Konfigurasi Global PostgreSQL & JDBC
PG_CONF = get_pg_conf()
JDBC_URL = f"jdbc:postgresql://{PG_CONF['host']}:{PG_CONF['port']}/{PG_CONF['dbname']}"
PROPS = {
    "user": PG_CONF["user"], 
    "password": PG_CONF["password"], 
    "driver": "org.postgresql.Driver", 
    "stringtype": "unspecified"
}

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
        conn = psycopg2.connect(
            host="postgres", 
            port=5432, 
            dbname="retail_analytics", 
            user="retail", 
            password="retail"
        )
    conn.autocommit = True
    return conn

def get_spark(app="SILVER_MEDALLION", **kwargs):
    return (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


# =========================================================================
# ORKESTRASI PIPELINE SILVER (BRONZE -> SILVER -> QUALITY GATE)
# =========================================================================

def run_silver(**context):
    print("=== START SILVER DAG CALLABLE (FIXED & UNIFIED) ===")
    try:
        stage1_lineage()
        stage2_fill_silver()
        count_silver_rows()
        stage3_timestamp()
        stage4_normalization()
        stage5_rejection()
        stage6_dedup()
        stage7_identity()
        stage8_temporal()
        stage9_fk()
        stage10_late()
        stage11_quality_gate()         
        print("=== ALL SILVER PIPELINE STAGES COMPLETED SUCCESSFULLY ===")
        return "silver_full_ok"
    
    except Exception as e:
        print(f"🚨 Pipeline Silver Gagal pada eksekusi tahapan: {e}")
        raise

def build_silver_dag_callable(**context):
    return run_silver(**context)

def main(**context):
    return run_silver(**context)

if __name__ == "__main__":
    run_silver()
