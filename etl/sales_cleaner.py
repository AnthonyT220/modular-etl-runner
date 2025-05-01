import os
import shutil
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from utils.file_utils import compute_file_hash, is_file_loaded, log_file_load
from utils.logger import setup_logger
from utils.notifier import send_email

raw_folder = r'C:/data/raw_source'
cleansed_folder = r'C:/data/cleansed_source'
archive_folder = r'C:/data/raw_archive'
rejected_folder = r'C:/data/raw_rejected'
log_path = r"C:/data/log/loaded_files_log.csv"
status_log_path = r"C:/data/log/status_log.csv"
LOG_FILE = r"C:/data/log/sales_cleaner.log"


def wait_for_file_ready(file_path, timeout=10):
    """Wait for file to be readable (i.e., not locked by another process)."""
    import time
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with open(file_path, 'rb'):
                return True
        except PermissionError:
            time.sleep(0.5)
    raise TimeoutError(f"File not ready after {timeout} seconds: {file_path}")


logger = setup_logger("sales_cleaner", LOG_FILE)

def log_status(filename, status, message):
    log_entry = pd.DataFrame([{
        "filename": filename,
        "status": status,
        "message": message,
        "timestamp": pd.Timestamp.now()
    }])
    if os.path.exists(status_log_path):
        log_entry.to_csv(status_log_path, mode='a', header=False, index=False)
    else:
        log_entry.to_csv(status_log_path, mode='w', header=True, index=False)

def transform_file(file_path):
    df = pd.read_csv(file_path, delimiter='\t')
    col_to_duplicate = 'For Store No: 20'
    df[f"{col_to_duplicate}_copy"] = df[col_to_duplicate]

    column_name = df.columns[4]
    df[column_name] = column_name
    column_to_move = "For Store No: 20"
    new_position = 0
    cols = df.columns.tolist()
    cols.remove(column_to_move)
    cols.insert(new_position, column_to_move)
    df = df[cols]

    col_to_duplicate = 'Unnamed: 0'
    df["Transaction Date"] = df[col_to_duplicate]
    df['Transaction Date'] = df['Transaction Date'].apply(lambda x: x if 'Transaction Totals:' in str(x) else "")
    df['Transaction Date'] = df['Transaction Date'].str.replace(r'\s*Transaction Totals:', '', regex=True)
    df.loc[0:1, 'Transaction Date'] = 'Transaction Date'
    df['Transaction Date'] = df['Transaction Date'].replace('', None)
    df['Transaction Date'] = df['Transaction Date'][::-1].ffill()[::-1]

    df.columns = df.iloc[1]
    df = df.iloc[2:].reset_index(drop=True)
    df.rename(columns={df.columns[0]: "Store No"}, inplace=True)
    df['Store No'] = df['Store No'].str.replace(r'\s*For Store No:', '', regex=True)

    df['Trns No'] = df['Trns No'].fillna("")
    df = df[df['Trns No'] != ""].reset_index(drop=True)

    df['A/R Amount'] = df['A/R Amount'].str.replace(r'0.00 \*', '0', regex=True)
    df['A/R Amount'] = df['A/R Amount'].str.replace(r'\*', '', regex=True).str.strip()

    columns_to_replace = ['Non Taxable Merch', 'Taxable Non-Merch', 'Non Tax Non Merch', 'Restock Charge', 
                          'Sales Tax', 'Cash Amount', 'Check Amount', 'Bank Card Amt', 'Refund Amount', 
                          'Applied Amount', 'Adjusted Amount', 'Exchange', 'Financed', 'Exception', 
                          'Taxable Merch']
    df[columns_to_replace] = df[columns_to_replace].apply(pd.to_numeric, errors='coerce').fillna(0)

    column_to_move = "Transaction Date"
    cols = df.columns.tolist()
    cols.remove(column_to_move)
    cols.insert(0, column_to_move)
    df = df[cols]

    columns_to_format = ["Taxable Merch", "Non Taxable Merch", "Taxable Non-Merch", "Non Tax Non Merch", 
                         "Restock Charge", "Sales Tax"]
    df[columns_to_format] = df[columns_to_format].apply(pd.to_numeric, errors="coerce").round(2)
    df[columns_to_format] = df[columns_to_format] * -1

    df['Written Sales Total'] = df[['Taxable Merch', 'Non Taxable Merch', 'Taxable Non-Merch', 
                                    'Non Tax Non Merch', 'Restock Charge']].sum(axis=1)
    df['Written Sales Grand Total'] = df[['Written Sales Total', 'Sales Tax']].sum(axis=1)

    column_order = [
        "Transaction Date", "Store No", "Customer Name (ID)", "Trns No", "Online Trans", 
        "Trans Desc", "Taxable Merch", "Non Taxable Merch", "Taxable Non-Merch", 
        "Non Tax Non Merch", "Restock Charge", "Written Sales Total", "Sales Tax", 
        "Written Sales Grand Total"
    ]
    df = df[column_order]

    valid_dates = pd.to_datetime(df['Transaction Date'], format='%m/%d/%y', errors='coerce')
    first_valid_date = valid_dates.dropna().iloc[0]
    date_str = first_valid_date.strftime('%Y-%m-%d')
    filename = f"sales_data_{date_str}.csv"
    export_path = os.path.join(cleansed_folder, filename)
    df.to_csv(export_path, index=False)

    return filename, first_valid_date, export_path

def process_sales_files():
    notifications = []
    for filename in os.listdir(raw_folder):
        if filename.endswith('.txt'):
            file_path = os.path.join(raw_folder, filename)
            try:
                file_hash = compute_file_hash(file_path)
                if is_file_loaded(file_hash, log_path):
                    log_status(filename, "skipped", "File already processed")
                    shutil.move(file_path, os.path.join(rejected_folder, filename))
                    notifications.append((filename, "skipped", "File already processed"))
                    continue
                output_filename, transaction_date, output_path = transform_file(file_path)
                log_file_load(output_filename, file_hash, transaction_date, log_path)
                log_status(filename, "success", f"Transformed and saved as {output_filename}")
                shutil.move(file_path, os.path.join(archive_folder, filename))
            except Exception as e:
                log_status(filename, "failed", str(e))
                shutil.move(file_path, os.path.join(rejected_folder, filename))
                notifications.append((filename, "failed", str(e)))

    if notifications:
        subject = "[Data Pipeline] Skipped/Failed/Rejection Summary"
        body = "The following files were skipped, failed, or rejected:\n\n"
        for fname, status, msg in notifications:
            body += f"- {fname} [{status}]: {msg}\n"
        send_email(subject, body)

class SalesDataHandler(FileSystemEventHandler):
    def on_created(self, event):
        wait_for_file_ready(event.src_path)
        if not event.is_directory and event.src_path.endswith(".txt"):
            logger.info(f"New sales file detected: {event.src_path}")
            try:
                filename = os.path.basename(event.src_path)
                file_hash = compute_file_hash(event.src_path)
                if is_file_loaded(file_hash, log_path):
                    log_status(filename, "skipped", "File already processed")
                    shutil.move(event.src_path, os.path.join(rejected_folder, filename))
                    return
                output_filename, transaction_date, output_path = transform_file(event.src_path)
                log_file_load(output_filename, file_hash, transaction_date, log_path)
                log_status(filename, "success", f"Transformed and saved as {output_filename}")
                shutil.move(event.src_path, os.path.join(archive_folder, filename))
            except Exception as e:
                log_status(filename, "failed", str(e))
                shutil.move(event.src_path, os.path.join(rejected_folder, filename))


def start_sales_watcher():
    os.makedirs(raw_folder, exist_ok=True)
    logger.info(f"Watching sales data folder: {raw_folder}")
    event_handler = SalesDataHandler()
    observer = Observer()
    observer.schedule(event_handler, raw_folder, recursive=False)
    observer.start()
    return observer
