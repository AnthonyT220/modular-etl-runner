import pandas as pd

def run_etl(file_path):
    """
    ETL process for inbound inventory files.
    - Reads the raw .tsv or .txt file
    - Cleans and formats data
    - Returns a cleaned DataFrame
    """
    df = pd.read_csv(file_path, sep='\t', engine='python')
    df.columns = df.columns.str.strip()

    # Example column clean-up
    df['ETA Date'] = pd.to_datetime(df.get('ETA Date'), errors='coerce')
    df['Qty'] = pd.to_numeric(df.get('Qty'), errors='coerce')

    return df
