-- Drop the table if it already exists (optional safety for dev environments)
DROP TABLE IF EXISTS etl_log;

-- Create the new etl_log table
CREATE TABLE etl_log (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    filename TEXT NOT NULL,
    report_date DATE,
    transaction_date DATE,
    row_count INTEGER DEFAULT 0,
    status TEXT,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Optional: Add an index to speed up duplicate checks
CREATE INDEX idx_etl_log_table_file ON etl_log(table_name, filename);
