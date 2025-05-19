-- Drop the table if it already exists
DROP TABLE IF EXISTS inbound_shipments;

-- Create the inbound_inventory table
CREATE TABLE inbound_shipments (
    "Vendor No" TEXT,
    "Stock No" TEXT,
    "Product Name" TEXT,
    "Division" TEXT,
    "Dept" TEXT,
    "ETA Week" TEXT,
    "ETA Date" DATE,
    "Rqst Ship Date" DATE,
    "Confirm Date" DATE,
    "Qty" INTEGER,
    "Po No" TEXT,
    "Container No" TEXT,
    "Confirmation No" TEXT,
    "Last Cost" NUMERIC(12, 2),
    "Item Create Date" DATE,
    "Avail Qty" INTEGER,
    "Report Date" DATE,
    "PO Create Date" DATE
);
