-- Drop existing table
DROP TABLE IF EXISTS daily_detail_sales;

-- Recreate with new column structure and types
CREATE TABLE daily_detail_sales (
    transaction_date DATE,
	store_no TEXT,
	customer_name_id TEXT,
    trns_no NUMERIC(12,0),
    online_trans TEXT,
    trans_desc TEXT,
    taxable_merch NUMERIC(12,2),
    non_taxable_merch NUMERIC(12,2),
    taxable_nonmerch NUMERIC(12,2),
    non_tax_non_merch NUMERIC(12,2),
    restock_charge NUMERIC(12,2),
	total_written_sales NUMERIC(12,2),
    sales_tax NUMERIC(12,2),
    grand_total_written_sales NUMERIC(12,2),
    cash_amount NUMERIC(12,2),
    check_amount NUMERIC(12,2),
    bank_card_amt NUMERIC(12,2),
    payment_type_code TEXT,
    refund_amount NUMERIC(12,2),
    applied_amount NUMERIC(12,2),
    adjusted_amount NUMERIC(12,2),
    ar_amount NUMERIC(12,2),
    exchange NUMERIC(12,2),
    financed NUMERIC(12,2),
    exception NUMERIC(12,2),
    invoice_date DATE,
    ship_qty NUMERIC(12,0)
);

