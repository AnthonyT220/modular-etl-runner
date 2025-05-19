import pandas as pd
from io import StringIO
import re

def clean_columns(df):
    """Clean column names by removing non-printable characters and normalizing whitespace."""
    df.columns = df.columns.map(lambda x: re.sub(r'\s+', ' ', re.sub(r'[^\x20-\x7E]', '', str(x))).strip())
    return df

def run_etl(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Detect header line
        header_index = next(i for i, line in enumerate(lines) if line.strip().startswith("Vendor No"))
        content = ''.join(lines[header_index:])
        df = pd.read_csv(StringIO(content), sep='\t', engine='python')

        # Clean columns and drop empty ones
        df = clean_columns(df)
        df = df.loc[:, ~df.columns.str.contains("^Unnamed", na=False)]

        if df.columns.duplicated().any():
            print("❌ Duplicate column names detected. Aborting upload.")
            return pd.DataFrame()

        # Clean string values
        df = df.map(lambda x: re.sub(r'\s+', ' ', str(x)).strip() if isinstance(x, str) else x)

        # Parse date columns (infer common formats like m/d/yyyy)
        date_fields = [
            "ETA Date", "Rqst Ship Date", "Confirm Date",
            "Item Create Date", "Report Date", "PO Create Date"
        ]
        for col in date_fields:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Parse numeric fields
        numeric_fields = ["Qty", "Last Cost", "Avail Qty"]
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Match DB schema
        expected_columns = [
            "Vendor No", "Stock No", "Product Name", "Division", "Dept", "ETA Week",
            "ETA Date", "Rqst Ship Date", "Confirm Date", "Qty", "Po No", "Container No",
            "Confirmation No", "Last Cost", "Item Create Date", "Avail Qty", "Report Date", "PO Create Date"
        ]

        missing = [col for col in expected_columns if col not in df.columns]
        if missing:
            print(f"❌ Missing expected columns: {missing}. Aborting.")
            print("🔍 Available columns:", df.columns.tolist())
            return pd.DataFrame()

        df = df[expected_columns]  # enforce order
        print("🧪 Final row count:", len(df))
        print("✅ Final columns ready for upload:", list(df.columns))
        print(df.head())

        print(f"🧪 Row count after all processing: {len(df)}")
        print("🧪 Final DataFrame preview:")
        print(df.head(3))

        return df

    except Exception as e:
        print(f"❌ Failed to process file {file_path}: {e}")
        return pd.DataFrame()
