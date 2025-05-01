
import hashlib
import pandas as pd
import os

def compute_file_hash(filepath, algo='sha256'):
    hash_func = hashlib.new(algo)
    with open(filepath, 'rb') as f:
        while chunk := f.read(8192):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def is_file_loaded(file_hash, log_path):
    if os.path.exists(log_path):
        log_df = pd.read_csv(log_path)
        return file_hash in log_df['file_hash'].values
    return False

def log_file_load(filename, file_hash, transaction_date, log_path):
    log_entry = pd.DataFrame([{
        "filename": filename,
        "file_hash": file_hash,
        "transaction_date": transaction_date,
        "load_timestamp": pd.Timestamp.now()
    }])
    if os.path.exists(log_path):
        log_entry.to_csv(log_path, mode='a', header=False, index=False)
    else:
        log_entry.to_csv(log_path, mode='w', header=True, index=False)
