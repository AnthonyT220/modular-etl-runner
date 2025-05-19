import os
import psycopg2
from etl.daily_detail_sales_etl import run_etl as run_sales_etl
from etl.inbound_shipments_etl import run_etl as run_inbound_shipments_etl
from utils.postgres_uploader import upload_to_postgres
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()


# === Pipeline Configuration ===

pipeline_map = {
    "daily_detail_sales": run_sales_etl,
    "inbound_shipments": run_inbound_shipments_etl,
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
        """, (
            table_name,
            filename,
            report_date,
            row_count,
            status,
            from datetime import datetime, timezone
            datetime.now(timezone.utc)

        ))
    conn.commit()

# === Main Runner ===

def main():
    base_folder = "data_files"

    # Read DB connection info from environment or use fallback
    conn = psycopg2.connect(
        dbname=os.getenv("PG_DATABASE"),  # was PG_DB
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432")
    )


    for pipeline_key, etl_function in pipeline_map.items():
        folder_path = os.path.join(base_folder, pipeline_key, "incoming")

        if not os.path.isdir(folder_path):
            print(f"⚠️ Skipping missing folder: {folder_path}")
            continue

        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)

            if not os.path.isfile(file_path):
                continue

            try:
                print(f"🟡 Running pipeline: {pipeline_key} for file: {file_name}")
                df = etl_function(file_path)

                # 🔍 Debug output
                print(f"🧪 DataFrame returned from ETL: {type(df)}")
                if df is not None:
                    print(f"🧪 Shape: {df.shape}")
                    print(f"🧪 Columns: {df.columns.tolist()}")
                    print(df.head(2))
                else:
                    print("🧪 No DataFrame returned (None)")
                    
                if df is not None and not df.empty:
                    print(f"✅ Transformed {len(df)} rows. Uploading to Postgres...")
                    upload_to_postgres(df, table_name=pipeline_key)

                    report_date = df["Report Date"].iloc[0] if "Report Date" in df.columns else None

                    log_etl_file(
                        pipeline_key,
                        file_name,
                        report_date=report_date,
                        row_count=len(df),
                        status="success",
                        conn=conn
                    )
                    print(f"✅ Upload complete.\n")
                else:
                    print(f"⚠️ No data to upload for {file_name}\n")
                    log_etl_file(
                        pipeline_key,
                        file_name,
                        report_date=None,
                        row_count=0,
                        status="empty",
                        conn=conn
                    )

            except Exception as e:
                print(f"❌ Error processing {file_name}: {e}")
                log_etl_file(
                    pipeline_key,
                    file_name,
                    report_date=None,
                    row_count=0,
                    status=f"failed: {str(e)[:200]}",
                    conn=conn
                )

    conn.close()

if __name__ == "__main__":
    main()
