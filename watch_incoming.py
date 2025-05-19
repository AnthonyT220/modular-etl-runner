from dotenv import load_dotenv
load_dotenv()

import os
import shutil
import time
import psycopg2
import pandas as pd
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from etl.daily_detail_sales_etl import run_etl as run_sales_etl
from etl.inbound_inventory_etl import run_etl as run_inventory_etl
from etl.inbound_shipments_etl import run_etl as run_shipments_etl
from utils.postgres_uploader import upload_to_postgres

# === CONFIG ===
WATCH_PATHS = {
    "daily_detail_sales": "data_files/daily_detail_sales/incoming",
    "inbound_inventory": "data_files/inbound_inventory/incoming",
    "inbound_shipments": "data_files/inbound_shipments/incoming"
}

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "database": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "port": os.getenv("PG_PORT", "5432")
}

# === HANDLER ===
class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith((".txt", ".tsv", ".csv")):
            return

        for table_name, watch_dir in WATCH_PATHS.items():
            if event.src_path.startswith(os.path.abspath(watch_dir)):
                file_path = event.src_path
                file_name = os.path.basename(file_path)
                print(f"📁 Detected new file: {file_name} for table {table_name}")

                if is_file_already_loaded(table_name, file_name):
                    print(f"⚠️ Skipping duplicate file: {file_name}")
                    log_etl_load(table_name, file_name, report_date=None, row_count=0, status="duplicate")
                    safe_move_file(file_path, table_name, "rejected")
                    return

                etl_func_map = {
                    "daily_detail_sales": run_sales_etl,
                    "inbound_inventory": run_inventory_etl,
                    "inbound_shipments": run_shipments_etl
                }
                etl_func = etl_func_map[table_name]


                try:
                    print(f"🧪 Using ETL function: {etl_func.__name__}")

                    df = etl_func(file_path)

                    if df is not None and not df.empty:
                        print(f"✅ Parsed {len(df)} rows. Uploading to {table_name}...")
                        upload_to_postgres(df, table_name)

                        report_date = df["Report Date"].iloc[0] if "Report Date" in df.columns else None
                        log_etl_load(table_name, file_name, report_date=report_date, row_count=len(df), status="success")
                        safe_move_file(file_path, table_name, "processed")
                        print(f"✅ Finished processing {file_name}\n")
                    else:
                        log_etl_load(table_name, file_name, row_count=0, status="empty")
                        safe_move_file(file_path, table_name, "rejected")
                        print(f"⚠️ No data found. Moved to rejected.\n")

                except Exception as e:
                    log_etl_load(table_name, file_name, status="error")
                    safe_move_file(file_path, table_name, "rejected")
                    print(f"❌ Error processing {file_name}: {e}")
                break

# === HELPERS ===
def is_file_already_loaded(table, filename):
    query = """
        SELECT 1 FROM etl_log
        WHERE table_name = %s AND filename = %s AND status = 'success'
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (table, filename))
            return cur.fetchone() is not None

def log_etl_load(table, filename, report_date=None, row_count=None, status="success"):
    query = """
        INSERT INTO etl_log (table_name, filename, report_date, row_count, status, load_time)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (table, filename, report_date, row_count, status))
        conn.commit()

def safe_move_file(src_path, pipeline_key, subfolder):
    target_dir = os.path.join("data_files", pipeline_key, subfolder)
    os.makedirs(target_dir, exist_ok=True)

    file_name = os.path.basename(src_path)
    dest_path = os.path.join(target_dir, file_name)

    if os.path.exists(dest_path):
        name, ext = os.path.splitext(file_name)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        dest_path = os.path.join(target_dir, f"{name}_{timestamp}{ext}")

    shutil.move(src_path, dest_path)

# === START WATCHING ===
def start_watching():
    observer = Observer()
    for key, path in WATCH_PATHS.items():
        abs_path = os.path.abspath(path)
        os.makedirs(abs_path, exist_ok=True)
        observer.schedule(NewFileHandler(), path=abs_path, recursive=False)
        print(f"👀 Watching: {abs_path}")

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_watching()
