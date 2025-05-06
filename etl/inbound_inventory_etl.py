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

        # Extract report date from banner
        banner_line = next(line for line in lines if "-*- INCOMING INVENTORY ITEMS REPORT -*-" in line)
        report_date_str = banner_line.strip().split(" -*-")[0].strip()

        try:
            report_date = pd.to_datetime(report_date_str, format='%d %b %Y')
            print(f"📅 Parsed Report Date: {report_date}")
        except Exception as e:
            print(f"❌ Failed to parse Report Date: '{report_date_str}' — {e}")
            report_date = pd.NaT

        # Remove banner lines and load content
        cleaned_lines = [line for line in lines if "-*- INCOMING INVENTORY ITEMS REPORT -*-" not in line]
        header_index = next(i for i, line in enumerate(cleaned_lines) if line.strip().startswith("Vendor#"))
        content = ''.join(cleaned_lines[header_index:])
        df = pd.read_csv(StringIO(content), sep='\t', engine='python')

        # Clean column names and drop unnamed
        df = clean_columns(df)
        df = df.loc[:, ~df.columns.str.contains("^Unnamed", na=False)]

        # Warn and exit if duplicate column names exist
        if df.columns.duplicated().any():
            print("❌ Duplicate column names detected. Aborting upload.")
            return pd.DataFrame()

        # Clean string fields
        df = df.map(lambda x: re.sub(r'\s+', ' ', str(x)).strip() if isinstance(x, str) else x)

        # Field-specific cleaning
        if "ETA Week" in df.columns:
            df["ETA Week"] = df["ETA Week"].str.replace("WK", "", regex=False).str.strip()

        if "Container No" in df.columns:
            df["Container No"] = df["Container No"].str.replace(r"^[CD]", "", regex=True).str.strip()

        # Add Report Date
        df["Report Date"] = report_date

        # Convert types
        df["ETA Date"] = pd.to_datetime(df.get("ETA Date"), errors="coerce")
        df["Qty"] = pd.to_numeric(df.get("Qty"), errors="coerce")

        # Expected final column structure (match to table schema!)
        expected_columns = [
            'Vendor#', 'Stock No.', 'Product Name', 'Division', 'Dept.', 'ETA Week',
            'ETA Date', 'Rqst Ship Date', 'Confirm Date', 'Qty', 'Po#', 'Container No',
            'Confirmation No', 'Last Cost', 'Date Created', 'PO Date', 'Report Date'
        ]

        missing = [col for col in expected_columns if col not in df.columns]
        if missing:
            print(f"❌ Missing expected columns: {missing}. Aborting.")
            return pd.DataFrame()

        df = df[expected_columns]  # enforce column order

        print("✅ Final columns ready for upload:", list(df.columns))
        print(df.head())

        return df

    except Exception as e:
        print(f"❌ Failed to process file {file_path}: {e}")
        return pd.DataFrame()
