CREATE TABLE IF NOT EXISTS etl_log (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    filename TEXT NOT NULL,
    load_timestamp TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (table_name, filename)
);
