import os
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sqlalchemy import create_engine
from utils.logger import setup_logger
from utils.file_utils import compute_file_hash, is_file_loaded, log_file_load
from utils.notifier import send_email

WATCH_DIR = r"C:/data/cleansed_source"
ARCHIVE_DIR = r"C:/data/cleansed_archive"
REJECTED_DIR = r"C:/data/cleansed_rejected"
LOG_PATH = r"C:/data/log/loaded_files_log.csv"
DB_URI = "postgresql+psycopg2://postgres:ScanDania@localhost:5432/erp_sales_data_db"
TABLE_NAME = "erp_processed_data2"
LOG_FILE = "C:/data/log/erp_loader.log"


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


logger = setup_logger("erp_loader", LOG_FILE)
engine = create_engine(DB_URI)

def process_erp_file(file_path):
    wait_for_file_ready(file_path)
    try:
        time.sleep(2)
        filename = os.path.basename(file_path)
        file_hash = compute_file_hash(file_path)
        if is_file_loaded(file_hash, LOG_PATH):
            logger.info(f"Skipping already processed file: {filename}")
            rejected_path = os.path.join(REJECTED_DIR, filename)
            os.makedirs(REJECTED_DIR, exist_ok=True)
            if os.path.exists(rejected_path):
                os.remove(rejected_path)
            os.rename(file_path, rejected_path)
            logger.info(f"Moved skipped file to rejected folder: {rejected_path}")
            return

        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.endswith((".xls", ".xlsx")):
            df = pd.read_excel(file_path)
        else:
            logger.warning(f"Unsupported file type: {file_path}")
            return

        logger.info(f"Processing file: {file_path}")
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df.drop_duplicates(inplace=True)
        df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
        logger.info(f"Inserted into DB: {file_path}")

        log_file_load(filename, file_hash, pd.Timestamp.now(), LOG_PATH)

        archive_path = os.path.join(ARCHIVE_DIR, filename)
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        if os.path.exists(archive_path):
            os.remove(archive_path)
        os.rename(file_path, archive_path)
        logger.info(f"Archived file to: {archive_path}")

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        rejected_path = os.path.join(REJECTED_DIR, os.path.basename(file_path))
        os.makedirs(REJECTED_DIR, exist_ok=True)
        if os.path.exists(rejected_path):
            os.remove(rejected_path)
        os.rename(file_path, rejected_path)
        logger.info(f"Moved errored file to rejected folder: {rejected_path}")
        subject = "[ERP Loader] File Processing Error"
        body = f"An error occurred while processing the file: {file_path}\n\nError: {str(e)}"
        send_email(subject, body)

class ERPDataHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            logger.info(f"New file detected: {event.src_path}")
            process_erp_file(event.src_path)

def start_erp_watcher():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    logger.info(f"Checking existing files in: {WATCH_DIR}")
    for file in os.listdir(WATCH_DIR):
        full_path = os.path.join(WATCH_DIR, file)
        if os.path.isfile(full_path):
            process_erp_file(full_path)

    logger.info(f"Watching directory: {WATCH_DIR}")
    event_handler = ERPDataHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIR, recursive=False)
    observer.start()
    return observer