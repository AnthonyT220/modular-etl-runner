import pandas as pd
import os

def run_etl(file_path):
    filename = os.path.basename(file_path)

    print(f"🚀 Starting Daily Sales Tax ETL for: {filename}")

    try:
        # Read file (skip first 2 rows)
        df = pd.read_csv(file_path, sep='\t', skiprows=2, dtype=str)
        df = df.dropna(how='all')  # Remove summary/footer rows

        # Normalize column headers
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('[^0-9a-zA-Z_]', '', regex=True)
        )

        # 🔽 Drop unnamed columns if present
        df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]

        # Expected structure safeguard
        expected_columns = [
            "store_no", "customer_name_id", "trns_no", "trns_date", "online_trans", "org_inv_no", "trans_desc",
            "sales_tax_rate", "delivery_address_1", "delivery_address_2", "delivery_city", "delivery_state", "delivery_zip_code",
            "taxable_merch", "non_taxable_merch", "taxable_nonmerch", "non_tax_non_merch", "restock_charge", "sales_tax"
        ]
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        # Transform types
        df["trns_date"] = pd.to_datetime(df["trns_date"], errors="coerce")
        numeric_cols = [
            "sales_tax_rate", "taxable_merch", "non_taxable_merch",
            "taxable_nonmerch", "non_tax_non_merch", "restock_charge", "sales_tax"
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 🔽 Replace NaNs with 0.00
        df[numeric_cols] = df[numeric_cols].fillna(0.00)

         # 🔽 Drop rows with blank transaction number
        df = df[df["trns_no"].notna() & (df["trns_no"].str.strip() != "")]

        # Add metadata
        df["filename"] = filename
        df["report_date"] = pd.to_datetime("today").date()

        # Rename to match DB schema
        df = df.rename(columns={
            "customer_name_id": "customer_name",
            "trns_no": "transaction_no",
            "trns_date": "transaction_date",
            "online_trans": "online_transaction",
            "org_inv_no": "original_invoice",
            "trans_desc": "transaction_description"
        })

        return df

    except Exception as e:
        print(f"❌ ETL error for {filename}: {e}")
        return None
