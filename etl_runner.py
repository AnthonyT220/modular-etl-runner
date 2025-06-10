import os
import psycopg2
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

from etl.daily_detail_sales_etl import run_etl as run_sales_etl
from etl.inbound_shipments_etl import run_etl as run_shipments_etl
from etl.daily_sales_tax_etl import run_etl as run_sales_tax_etl
from utils.postgres_uploader import upload_to_postgres

load_dotenv()

# === Pipeline Configuration ===
pipeline_map = {
    "daily_detail_sales": run_sales_etl,
    "inbound_shipments": run_shipments_etl,
    "daily_sales_tax": run_sales_tax_etl,
}

# === ETL Log Helpers ===
def file_already_loaded(table_name, filename, conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM etl_log WHERE table_name = %s AND filename = %s
        """, (table_name, filename))
        return cur.fetchone() is not None

def log_etl_file(table_name, filename, report_date, row_count, status, conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO etl_log (table_name, filename, report_date, row_count, status, load_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (table_name, filename) DO UPDATE
            SET status = EXCLUDED.status,
                report_date = EXCLUDED.report_date,
                row_count = EXCLUDED.row_count,
                load_time = EXCLUDED.load_time
        """, (
            table_name,
            filename,
            report_date,
            row_count,
            status,
            datetime.now(timezone.utc)
        ))
    conn.commit()

# === File Move Helpers ===
def move_file(file_path, target_folder):
    os.makedirs(target_folder, exist_ok=True)
    target_path = os.path.join(target_folder, os.path.basename(file_path))
    os.rename(file_path, target_path)

# === Main Runner ===
def main():
    base_folder = "data_files"

    conn = psycopg2.connect(
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432")
    )

    for pipeline_key, etl_function in pipeline_map.items():
        incoming_path = os.path.join(base_folder, pipeline_key, "incoming")

        if not os.path.isdir(incoming_path):
            print(f"⚠️ Skipping missing folder: {incoming_path}")
            continue

        for file_name in os.listdir(incoming_path):
            file_path = os.path.join(incoming_path, file_name)

            if not os.path.isfile(file_path):
                continue

            if file_already_loaded(pipeline_key, file_name, conn):
                print(f"⏩ Already processed: {file_name}")
                continue

            try:
                print(f"🟡 Running pipeline: {pipeline_key} for file: {file_name}")
                df = etl_function(file_path)

                if df is not None and not df.empty:
                    try:
                        upload_to_postgres(df, table_name=pipeline_key)
                        report_date = df["report_date"].iloc[0] if "report_date" in df.columns else None
                        log_etl_file(pipeline_key, file_name, report_date, len(df), "success", conn)
                        move_file(file_path, incoming_path.replace("incoming", "processed"))
                        print(f"✅ {file_name} processed successfully.\n")
                    except Exception as upload_err:
                        log_etl_file(pipeline_key, file_name, None, 0, f"failed: {str(upload_err)[:200]}", conn)
                        move_file(file_path, incoming_path.replace("incoming", "rejected"))
                        print(f"❌ Upload failed for {file_name}: {upload_err}\n")

                    move_file(file_path, incoming_path.replace("incoming", "processed"))
                    print(f"✅ {file_name} processed successfully.\n")

                else:
                    log_etl_file(pipeline_key, file_name, None, 0, "empty", conn)
                    move_file(file_path, incoming_path.replace("incoming", "rejected"))
                    print(f"⚠️ No data to upload for {file_name} — moved to rejected.\n")

            except Exception as e:
                log_etl_file(pipeline_key, file_name, None, 0, f"failed: {str(e)[:200]}", conn)
                move_file(file_path, incoming_path.replace("incoming", "rejected"))
                print(f"❌ Error processing {file_name}: {e}\n")

    conn.close()

if __name__ == "__main__":
    main()
