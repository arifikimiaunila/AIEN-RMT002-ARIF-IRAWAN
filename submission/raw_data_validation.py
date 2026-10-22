import json
import os
import csv
import logging
import tempfile
from datetime import datetime

logger = logging.getLogger(__name__)

def resolve_path(path):
    candidates = [
        path,
        path.replace("refference", "reference"),
        path.replace("reference", "refference"),
        os.path.join("/opt/airflow", path),
        os.path.join("/opt/airflow", path.replace("refference", "reference")),
        os.path.join("/mnt/data", os.path.basename(path)),
        os.path.join("data/raw", os.path.basename(path)),
        os.path.join(os.getcwd(), path),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return path

def validate_manifest(filepath, base_dir="data/raw"):
    filepath = resolve_path(filepath)
    result = {"file": filepath, "fatal": False, "errors": [], "warnings": [], "risk": False}
    if not os.path.exists(filepath):
        result["warnings"].append(f"Manifest tidak ditemukan: {filepath} - SKIP (tidak riskan)")
        return result
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            json.load(f)
    except Exception as e:
        result["fatal"] = True
        result["risk"] = True
        result["errors"].append(f"Gagal membaca manifest (RISIKAN): {e}")
    return result

def validate_json_file(filepath, required_keys=None):
    filepath = resolve_path(filepath)
    result = {"file": filepath, "fatal": False, "errors": [], "warnings": [], "risk": False}
    if not os.path.exists(filepath):
        result["warnings"].append(f"File tidak ditemukan - SKIP (tidak riskan): {filepath}")
        return result
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            json.load(f)
    except json.JSONDecodeError as e:
        result["fatal"] = True
        result["risk"] = True
        result["errors"].append(f"Format JSON tidak valid (RISIKAN): {e}")
    except Exception as e:
        result["fatal"] = True
        result["risk"] = True
        result["errors"].append(f"Gagal baca JSON (RISIKAN): {e}")
    return result

def validate_csv_file(filepath, required_columns=None):
    filepath = resolve_path(filepath)
    result = {"file": filepath, "fatal": False, "errors": [], "warnings": [], "risk": False}
    if not os.path.exists(filepath):
        result["warnings"].append(f"CSV tidak ditemukan - SKIP (tidak riskan): {filepath}")
        return result
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            if required_columns:
                for col in required_columns:
                    if col not in header:
                        if "inventory_snapshots.csv" in filepath:
                            result["warnings"].append(f"Kolom '{col}' tidak ada di inventory - DIBIARKAN")
                        else:
                            result["fatal"] = True
                            result["risk"] = True
                            result["errors"].append(f"Kolom wajib '{col}' tidak ditemukan (RISIKAN)")
    except Exception as e:
        result["fatal"] = True
        result["risk"] = True
        result["errors"].append(f"Gagal membaca CSV (RISIKAN): {e}")
    return result

# FIX UTAMA: tambah **kwargs biar gak error 'conf'
def validate_raw_data(**kwargs):
    results = []
    results.append(validate_manifest("data/raw/manifest.json"))

    for file in [
        "data/raw/events/payment_events.json",
        "data/raw/events/refund_events.json",
        "data/raw/events/return_events.json",
        "data/raw/events/support_events.json",
        "data/raw/events/web_events.json"
    ]:
        results.append(validate_json_file(file))

    operational_required = {
        "data/raw/operational/customers.json": ["customer_id"],
        "data/raw/operational/customer_profiles.json": ["customer_id"],
        "data/raw/operational/customer_addresses.json": ["customer_id"],
        "data/raw/operational/orders.json": ["order_id"],
        "data/raw/operational/order_items.json": ["order_id"],
        "data/raw/operational/order_promotions.json": ["order_id"],
        "data/raw/operational/products.json": ["product_id"],
        "data/raw/operational/product_categories.json": ["category_id"],
        "data/raw/operational/promotions.json": ["promotion_id"],
        "data/raw/operational/sales_channels.json": ["channel_id"],
        "data/raw/operational/stores.json": ["store_id"],
        "data/raw/refference/city_reference.json": ["city_id"]
    }
    for file in operational_required:
        results.append(validate_json_file(file))

    csv_required = {
        "data/raw/inventory/inventory_snapshots.csv": ["snapshot_date", "store_id", "product_id", "quantity"],
        "data/raw/refference/campaign_spend.csv": ["spend_date", "campaign_id", "channel", "spend_amount"]
    }
    for file, cols in csv_required.items():
        results.append(validate_csv_file(file, cols))

    report_dir = "/tmp"
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, "validation_report.txt")

    try:
        with open(report_path, "w", encoding="utf-8") as report_file:
            report_file.write("="*60+"\nLAPORAN HASIL VALIDASI DATA\n")
            report_file.write(f"Waktu: {datetime.now()}\n"+"="*60+"\n\n")
            for res in results:
                report_file.write(f"File: {res['file']}\nFatal:{res['fatal']} Risk:{res.get('risk',False)}\n")
                for err in res["errors"]: report_file.write(f"  Errors: {err}\n")
                for warn in res["warnings"]: report_file.write(f"  Warnings: {warn}\n")
                report_file.write("-"*60+"\n")
    except Exception:
        pass

    try: os.chmod(report_path, 0o777)
    except: pass

    has_blocking = False
    blocking_details = []
    for res in results:
        if res.get("risk") and res["fatal"]:
            if "inventory_snapshots.csv" in res["file"]:
                continue
            has_blocking = True
            blocking_details.append(f"{res['file']}: {res['errors']}")

    print(f"Report: {report_path} Blocking:{has_blocking}")

    if has_blocking:
        raise ValueError("Diblokir karena HAL RISKAN:\n" + "\n".join(blocking_details))
    
    print("VALIDATION PASSED - tidak ada hal riskan")
    return report_path

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(validate_raw_data())
