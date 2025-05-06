# Modular ETL Runner

This project automates the ingestion and transformation of structured sales and inventory files into a PostgreSQL database. It includes automatic detection of new files, real-time processing, validation, cleaning, and logging of ETL activity.

---

## ✅ Features

- Watches `incoming/` folders for new sales and inventory files
- Cleans, transforms, and validates `.txt` and `.tsv` tab-delimited reports
- Applies custom business rules for formatting and calculations
- Inserts structured records into PostgreSQL
- Maintains detailed ETL logs (success, duplicate, rejected)
- Auto-moves processed files to `processed/` or `rejected/` folders
- Auto-recreates Postgres tables if empty and schema mismatch is detected
- Compatible with `systemd` services on Linux servers

---

## 📁 Folder Structure

```
modular-etl-runner/
├── data_files/
│   ├── daily_detail_sales/
│   │   ├── incoming/
│   │   ├── processed/
│   │   └── rejected/
│   └── inbound_inventory/
│       ├── incoming/
│       ├── processed/
│       └── rejected/
├── etl/
│   ├── daily_detail_sales_etl.py
│   └── inbound_inventory_etl.py
├── sql/
│   ├── create_etl_log_table.sql
│   ├── create_inbound_inventory_table.sql
│   └── create_daily_detail_sales_table.sql
├── utils/
│   └── postgres_uploader.py
├── watch_incoming.py
├── etl_runner.py
├── .env
└── requirements.txt
```

---

## ⚙️ Setup Instructions (Linux Server)

### 1. Install Python and PostgreSQL

```bash
sudo apt update
sudo apt install python3 python3-pip postgresql postgresql-client
```

### 2. Clone the Repo

```bash
git clone https://github.com/yourusername/modular-etl-runner.git
cd modular-etl-runner
```

### 3. Install Requirements

```bash
pip install -r requirements.txt
```

### 4. Create the PostgreSQL Tables

```bash
psql -U your_pg_user -d your_db_name -h localhost -f sql/create_etl_log_table.sql
psql -U your_pg_user -d your_db_name -h localhost -f sql/create_inbound_inventory_table.sql
psql -U your_pg_user -d your_db_name -h localhost -f sql/create_daily_detail_sales_table.sql
```

### 5. Configure Environment Variables

Create a `.env` file in the root directory:

```ini
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=yourpassword
PG_DATABASE=modular_etl
```

---

## 🏁 Running the Watcher

```bash
python3 watch_incoming.py
```

To stop: `CTRL+C`

---

## 🛠 Run as a systemd Service

1. Copy `modular_etl.service` to `/etc/systemd/system/`
2. Enable and start:

```bash
sudo systemctl daemon-reexec
sudo systemctl enable modular_etl.service
sudo systemctl start modular_etl.service
```

---

## 🔍 Logs

- All activity is tracked in the `etl_log` table
- Duplicate files are skipped and logged
- Rejected files are moved to the `rejected/` folder

---

## 📦 Dependencies

See `requirements.txt` for full list.

---

## 🧪 Tested On

- Ubuntu 22.04
- Python 3.10+
- PostgreSQL 14+

---

## 📬 Questions?

Reach out to the project maintainer.