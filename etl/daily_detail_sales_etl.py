import pandas as pd
import os
from utils.postgres_uploader import upload_to_postgres

def run_etl(file_path):
    """
    Handles ETL for a single sales file.
    """
    df = pd.read_csv(file_path, sep='\t', engine='python')
    df.columns = df.columns.str.strip()
    df['Invoice Date'] = pd.to_datetime(df.get('Invoice Date'), errors='coerce')
    df['Ship Qty'] = pd.to_numeric(df.get('Ship Qty'), errors='coerce')
    return df

def process_sales_files(directory="data_files"):
    """
    Processes all sales files in a directory and uploads them to Postgres.
    """
    for filename in os.listdir(directory):
        if "sales" in filename.lower():
            file_path = os.path.join(directory, filename)
            print(f"Processing {file_path}")
            df = run_etl(file_path)

            if df is not None and not df.empty:
                upload_to_postgres(df, table_name="daily_detail_sales")
                print(f"✅ Uploaded {len(df)} rows from {filename}")
            else:
                print(f"⚠️ No data to upload from {filename}")
