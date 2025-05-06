-- Drop the table if it already exists
DROP TABLE IF EXISTS inbound_inventory;

-- Create the inbound_inventory table
CREATE TABLE inbound_inventory (
    "Vendor#" TEXT,
    "Stock No." TEXT,
    "Product Name" TEXT,
    "Division" TEXT,
    "Dept." TEXT,
    "ETA Week" TEXT,
    "ETA Date" DATE,
    "Rqst Ship Date" DATE,
    "Confirm Date" DATE,
    "Qty" INTEGER,
    "Po#" TEXT,
    "Container No" TEXT,
    "Confirmation No" TEXT,
    "Last Cost" NUMERIC(12, 2),
    "Date Created" DATE,
    "PO Date" DATE,
    "Report Date" DATE
);
