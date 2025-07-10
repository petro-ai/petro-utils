# Data Quality Control Script for Well Data

This Python script performs data extraction, validation, and quality control (QC) checks on oil & gas well data. It supports reading from a MySQL database or local files and provides detailed summary reports and optional cleaned outputs for multiple well-related tables.

## 📌 Features

- Connects to a MySQL database or reads local `.csv` or `.xlsx`files
- Loads and summarizes data for key upstream datasets:
  - `Well`, `WellExtra`, `MonthlyProduction`, `WellDirectionalSurveyPoint`
  - `WellLookup`, `GridStructureData`, `GridAttributeData`, etc.
- Executes QC checks:
  - Unique well ID counts
  - Column-level summaries
  - Date validation
  - Interval presence across datasets
- Optionally drops exact duplicates from MonthlyProduction
- Saves cleaned files (toggleable)
- Compares well IDs across tables for consistency

---

## 🗃 Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration-options)
- [QC Functionality](#qc-functionality)
- [Dependencies](#dependencies)
- [File Structure](#file-structure)
- [License](#license)

---

## 🧰 Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/your-org/well-data-qc.git
   cd well-data-qc
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## 🚀 Usage

Modify the configuration options in the script (e.g., `data_source`, `db_config`, `tables_to_check`, etc.) then run:

```bash
python PetroAI_source_data_qc.py
```

---

## ⚙️ Configuration Options

| Option                | Description                                                                 |
|-----------------------|-----------------------------------------------------------------------------|
| `data_source`         | `"database"` or `"files"` — determines source of input data                |
| `save_cleaned_files`  | If `True`, saves cleaned CSVs to `save_path`                                |
| `drop_duplicates`     | If `True`, drops exact duplicate production records                        |
| `compare`             | If `True`, checks wellId overlap across key datasets                        |
| `tables_to_check`     | List of tables to process (from a predefined set)                           |
| `srce_path`/`save_path` | Local path for loading and saving files (when using file input)           |

---

## 🔎 QC Functionality

The script uses helper functions from two custom modules:

- `RENAME_MAPPING.py`: defines standard column names
- `QC_functions.py`: contains reusable QC and reporting functions:
  - `process_well_data`
  - `summarize_well_data`
  - `date_checker`
  - `process_monProd_data`
  - `process_production_data`
  - `summarize_survey_data`
  - `find_unique_ids`
  - `check_interval_presence_and_count`, etc.

These functions generate summaries and help validate:
- Missing or inconsistent columns
- Invalid or missing dates
- Cross-table consistency for interval and wellId values

---

## 📦 Dependencies

- Python 3.7+
- `pandas`
- `mysql-connector-python`
- `openpyxl` (if using Excel files)

Install via:
```bash
pip install pandas mysql-connector-python openpyxl
```

---

## 📁 File Structure

```
├── PetroAI_source_data_qc.py   # Main script
├── QC_functions.py             # Shared functions for validation and reporting
├── RENAME_MAPPING.py           # Column mapping definitions
├── requirements.txt            # Python dependencies (optional)
├── README.md                   # This file
└── data/                       # (optional) Local data folder
```

---

## 🔐 Database Credentials

The script expects a MySQL database config block:
```python
db_config = {
    "host": "your-host",
    "user": "your-user",
    "password": "your-pass",
    "port": 3306,
    "database": "your-db"
}
```

> ⚠️ **Never commit passwords to GitHub.** Use environment variables or `.env` files with [python-dotenv](https://pypi.org/project/python-dotenv/) for security.

---

## 📄 License

This project is private or proprietary unless otherwise specified.
