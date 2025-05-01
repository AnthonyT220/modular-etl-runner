
from etl.erp_loader import start_erp_watcher
from etl.sales_cleaner import process_sales_files, start_sales_watcher
import time

if __name__ == "__main__":
    # Start watchers
    erp_observer = start_erp_watcher()
    sales_observer = start_sales_watcher()

    # Optionally run sales cleaner once on startup
    process_sales_files()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        erp_observer.stop()
        sales_observer.stop()
        erp_observer.join()
        sales_observer.join()

