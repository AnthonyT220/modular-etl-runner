import pandas as pd
from io import StringIO
import re

def run_etl(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Step 1: Extract Store No
        store_no = None
        for line in lines:
            match = re.search(r"For Store No:\s*(\d+)", line)
            if match:
                store_no = match.group(1)
                print(f"🏪 Parsed Store No: {store_no}")
                break

        if not store_no:
            store_no = "UNKNOWN"
            print("⚠️ Could not extract Store No")

        # Step 2: Find header row index
        header_index = next((i for i, line in enumerate(lines)
                            if "Trns No" in line and "Customer Name" in line), None)
        if header_index is None:
            print("❌ Could not find header row. Aborting.")
            return pd.DataFrame()

        # Step 3: Read CSV from header onward
        content = ''.join(lines[header_index:])
        df = pd.read_csv(StringIO(content), sep='\t', engine='python')
        df.columns = df.columns.str.strip()

        # Step 4: Remove footer notes (e.g., '* Indicates ...')
        df = df[~df.apply(lambda row: row.astype(str).str.contains(r"\* Indicates", case=False, na=False).any(), axis=1)]

        # Step 5: Extract transaction_date from markers
        if 'Customer Name (ID)' in df.columns:
            date_pattern = r'^(\d{2}/\d{2}/\d{2}) Transaction Totals:'
            df['transaction_date_marker'] = df['Customer Name (ID)'].str.extract(date_pattern)
            df['transaction_date'] = df['transaction_date_marker'][::-1].fillna(method='ffill')[::-1]
            df.drop(columns=['transaction_date_marker'], inplace=True)
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='%m/%d/%y', errors='coerce')
        else:
            print("⚠️ Could not find 'Customer Name (ID)' column to extract transaction_date")

        # Step 6a: Parse invoice and quantity columns
        df['Invoice Date'] = pd.to_datetime(df.get('Invoice Date'), errors='coerce')
        df['Ship Qty'] = pd.to_numeric(df.get('Ship Qty'), errors='coerce')

        # Step 6b: Drop rows where Trns No is null
        df = df[pd.to_numeric(df.get('Trns No'), errors='coerce').notnull()]

        # Step 6c: Remove asterisk from A/R Amount
        if 'A/R Amount' in df.columns:
            df['A/R Amount'] = df['A/R Amount'].astype(str).str.replace('*', '', regex=False).str.strip()

        # Step 6d: Flip sign for written sales-related fields
        flip_fields = [
            'Taxable Merch', 'Non Taxable Merch', 'Taxable Non-Merch',
            'Non Tax Non Merch', 'Restock Charge', 'Sales Tax'
        ]
        for field in flip_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0.0) * -1

        # Step 6e: Clean and convert other numeric fields
        more_numeric_fields = [
            'Cash Amount', 'Check Amount', 'Bank Card Amt', 'Refund Amount',
            'Applied Amount', 'Adjusted Amount', 'A/R Amount',
            'Exchange', 'Financed', 'Exception', 'Ship Qty'
        ]
        for field in more_numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0.0)

        # Step 7: Calculate Total Written Sales
        df['Total Written Sales'] = (
            df['Taxable Merch'] +
            df['Non Taxable Merch'] +
            df['Taxable Non-Merch'] +
            df['Non Tax Non Merch'] +
            df['Restock Charge']
        )

        # Step 7b: Calculate Grand Total Written Sales (includes Sales Tax)
        if 'Total Written Sales' in df.columns and 'Sales Tax' in df.columns:
            df['Grand Total Written Sales'] = df['Total Written Sales'] + df['Sales Tax']

        # Step 8: Add Store No
        df['Store No'] = store_no

        # Step 9: Drop completely empty rows
        df.dropna(how="all", inplace=True)

        print("✅ Final columns (before normalization):", list(df.columns))
        print(df.head())

        # Step 10: Normalize column names for Postgres
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r'[^\w\s]', '', regex=True)
            .str.replace(r'\s+', '_', regex=True)
        )

        print("🧼 Normalized columns:", list(df.columns))
        return df

    except Exception as e:
        print(f"❌ Failed to process file {file_path}: {e}")
        return pd.DataFrame()
