# utils/postgres_uploader.py

import pandas as pd
from sqlalchemy import create_engine

# TODO: Replace these with your real credentials or read from a .env/config
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "your_database"
DB_USER = "your_user"
DB_PASS = "your_password"

def get_engine():
    connection_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def upload_to_postgres(df: pd.DataFrame, table_name: str, if_exists: str = "append"):
    """
    Uploads a DataFrame to the specified Postgres table.
    
    Args:
        df: Pandas DataFrame to upload.
        table_name: Name of the target table in Postgres.
        if_exists: 'append', 'replace', or 'fail' (default = 'append')
    """
    try:
        engine = get_engine()
        df.to_sql(table_name, con=engine, index=False, if_exists=if_exists, method='multi')
        print(f"✅ Upload to Postgres complete: {table_name} ({len(df)} rows)")
    except Exception as e:
        print(f"❌ Failed to upload to Postgres: {e}")
