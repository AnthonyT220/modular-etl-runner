-- Drop the table if it already exists (optional safety for dev environments)
DROP TABLE IF EXISTS daily_sales_tax;

-- Create the new daily_sales_tax table
CREATE TABLE daily_sales_tax (
    store_no TEXT,
    customer_name TEXT,
    transaction_no TEXT,
    transaction_date DATE,
    online_transaction TEXT,
    original_invoice TEXT,
    transaction_description TEXT,
    sales_tax_rate NUMERIC(6, 5),
    delivery_address_1 TEXT,
    delivery_address_2 TEXT,
    delivery_city TEXT,
    delivery_state TEXT,
    delivery_zip_code TEXT,
    taxable_merch NUMERIC(12,2),
    non_taxable_merch NUMERIC(12,2),
    taxable_nonmerch NUMERIC(12,2),
    non_tax_non_merch NUMERIC(12,2),
    restock_charge NUMERIC(12,2),
    sales_tax NUMERIC(12,2),
    filename TEXT,
    report_date DATE,
    date_loaded TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);