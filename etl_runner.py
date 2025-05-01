import os
from etl.daily_detail_sales_etl import run_etl as run_sales_etl
from etl.inbound_inventory_etl import run_etl as run_inbound_inventory_etl
from utils.postgres_uploader import upload_to_postgres

# Map file types to their ETL logic
pipeline_map = {
    "daily_detail_sales": run_sales_etl,
    "inbound_inventory": run_inbound_inventory_etl,
    # Add more file types and ETL functions here
}

def detect_pipeline(file_name: str) -> str:
    """
    Determine which pipeline to run based on file name.
    You can update this logic to use config files or column sniffing if needed.
    """
    file_name_lower = file_name.lower()
    if "sales" in file_name_lower:
        return "daily_detail_sales"
    elif "inbound" in file_name_lower:
        return "inbound_inventory"
    else:
        raise ValueError(f"Unable to determine pipeline for file: {file_name}")

def main():
    data_folder = "data_files"  # Change if your folder is named differently

    for file_name in os.listdir(data_folder):
        file_path = os.path.join(data_folder, file_name)

        if not os.path.isfile(file_path):
            continue

        try:
            pipeline_key = detect_pipeline(file_name)
            print(f"🟡 Running pipeline: {pipeline_key} for file: {file_name}")

            etl_function = pipeline_map[pipeline_key]
            df = etl_function(file_path)

            if df is not None and not df.empty:
                print(f"✅ Transformed {len(df)} rows. Uploading to Postgres...")
                upload_to_postgres(df, table_name=pipeline_key)
                print(f"✅ Upload complete.\n")
            else:
                print(f"⚠️ No data to upload for {file_name}\n")

        except Exception as e:
            print(f"❌ Error processing {file_name}: {e}")

if __name__ == "__main__":
    main()
