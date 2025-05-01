
# Modular ETL Runner

A modular, scalable ETL (Extract, Transform, Load) framework built in Python. It processes multiple file types (e.g., sales orders, inbound inventory), transforms them as needed, and uploads the cleaned data into a PostgreSQL database.

---

## 🗂️ Project Structure

```
modular-etl-runner/
├── etl_runner.py               # Main controller that routes files to ETL modules
├── etl/
│   ├── daily_detail_sales_etl.py   # ETL logic for sales files
│   ├── inbound_inventory_etl.py    # ETL logic for inbound inventory files
│   └── erp_loader.py               # ERP-specific data extraction (if used)
├── utils/
│   ├── postgres_uploader.py       # Uploads DataFrames to PostgreSQL
│   └── file_loader.py             # Optional file handling utils
├── data_files/                # Directory for input files
├── requirements.txt           # Python dependencies
└── README.md
```

---

## 🚀 How It Works

1. Drop raw data files into the `data_files/` directory.
2. Run `etl_runner.py`.
3. The script detects the file type based on naming patterns and calls the appropriate ETL module.
4. Transformed data is uploaded to PostgreSQL.

---

## ▶️ Running the Pipeline

```bash
python etl_runner.py
```

This will:
- Loop through all files in `data_files/`
- Use matching rules (e.g., filenames containing "sales", "inbound")
- Route each to the corresponding ETL logic in `etl/`
- Upload results to a table in Postgres

---

## 🧠 Pipeline Detection Rules

The logic in `etl_runner.py` uses simple rules like:

| File contains... | Uses module...                | Loads to table...        |
|------------------|-------------------------------|--------------------------|
| `sales`          | `daily_detail_sales_etl.py`   | `daily_detail_sales`     |
| `inbound`        | `inbound_inventory_etl.py`    | `inbound_inventory`      |

More modules can be added by extending `etl/` and mapping them in the controller.

---

## 🔌 PostgreSQL Upload

Ensure your `postgres_uploader.py` script contains connection logic using a secure approach (e.g., environment variables or `.env` file).

Example interface:
```python
def upload_to_postgres(df, table_name: str) -> None:
    pass  # Upload logic here
```

---

## ➕ Adding a New Pipeline

1. Create a new file in `etl/`, like `new_type_etl.py`
2. Implement `run_etl(file_path)` to return a cleaned DataFrame
3. Update `etl_runner.py` to recognize files matching the new pattern
4. That’s it! You’ve added a new pipeline.

---

## 📦 Dependencies

Install requirements with:

```bash
pip install -r requirements.txt
```

---

## 👨‍💻 Maintained by

Anthony Turner
