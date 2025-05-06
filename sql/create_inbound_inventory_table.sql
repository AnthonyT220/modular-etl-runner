-- Drop the table if it already exists (optional for dev)
DROP TABLE IF EXISTS inbound_inventory;

-- Create the inbound_inventory table
CREATE TABLE inbound_inventory (
    vendor_id TEXT,
    stock_no TEXT,
    product_name TEXT,
    division TEXT,
    dept TEXT,
    eta_week TEXT,
    eta_date DATE,
    rqst_ship_date TEXT,
    confirm_date TEXT,
    qty INTEGER,
    po_no TEXT,
    container_no TEXT,
    confirmation_no TEXT,
    last_cost NUMERIC(12, 2),
    date_created TEXT,
    po_date TEXT,
    report_date DATE
);