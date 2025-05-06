-- Adds a unique constraint to prevent duplicate filename loads per table
ALTER TABLE etl_log
ADD CONSTRAINT etl_log_table_file_unique UNIQUE (table_name, filename);
