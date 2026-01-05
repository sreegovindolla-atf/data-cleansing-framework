# Data Cleansing & Extraction Framework

## Overview
This project is a modular **data extraction and post-processing framework** designed to:
- Extract structured data from unstructured or semi-structured text using AI-based extraction
- Store intermediate results as JSON / JSONL / CSV
- Post-process and normalize extracted data
- Load clean, structured data into relational database tables (SQL Server)

The framework is built to be **reproducible, auditable, and scalable**, making it suitable for data engineering, analytics, and reporting use cases.

---

## Architecture (High Level)

```text
Input Text / Documents
        ↓
AI Extraction (data_extraction.py)
        ↓
JSONL / JSON / CSV Outputs
        ↓
Post Processing (post_processing.py)
        ↓
Normalized SQL Tables
```
---

## Project Structure

```text
data-cleansing-framework/
│
├── src/
| ├── main.py
│ ├── data_extraction.py # AI-based extraction logic
│ ├── post_processing.py # Cleansing, normalization, DB writes
│
├── data/
| └── input/
|   └── denorm_mastertable.csv
│ └── outputs/
│   └── <run_id>/
│     ├── <run_id>_combined_extraction_results.jsonl
│     ├── <run_id>_combined_extraction.json
│     ├── <run_id>_combined_extraction.csv
|     └── <run_id>_lx_cache.pkl
│
├── config/
│ ├── prompt.py
| └── examples/
│   ├── distribution_projects.py
|   ├── infrastructure_projects.py
|   └── service_projects.py
|
├── utils/
│ ├── extraction_helpers.py
| ├── post_processing_helpers.py
| └── post_processing_sql_queries.py
│
├── .env
├── .gitignore
├── requirements.txt
├── LICENSE
└── README.md
```

---

## Prerequisites

Install dependencies:
```text
pip install -r requirements.txt
```

---

## 🚀 How to Run

You can run the framework in **two ways**:
1. Run each step individually
2. Run the entire pipeline using a single entry point (`main.py`)

---

### 1️⃣a Run Data Extraction (Standalone)

This step reads raw input text and produces structured extraction outputs.

```bash
python src/data_extraction.py --run-id <RUN_ID>
```
Generated outputs:

<run_id>_combined_extraction_results.jsonl

<run_id>_combined_extraction.json

<run_id>_combined_extraction.csv

### 1️⃣b Run Post Processing (Standalone)

This step reads extracted JSON / JSONL / CSV files, cleans and normalizes fields, and writes data into multiple SQL Server tables

```bash
python src/post_processing.py --run-id <RUN_ID>
```

### 2️⃣ Run End-to-End Pipeline (Recommended)
To run the entire workflow (extraction + post-processing) in one go, use the main entry point.
In this case, a run_id will be auto-generated using the current date and time
**Example auto-generated format:** 20251231_160609

```bash
python src/main.py
```

This will:
1. Execute data extraction
2. Generate intermediate output files (jsonl, json, csv)
3. Perform post-processing
4. Load cleaned data into SQL Server

---

## Database Design

The post-processing layer writes into separate normalized tables, for example:

Master projects table

Project details table

Project assets table

---

👤 Author

Sree Spoorthy G

Data Engineer

---

📜 License

This project is licensed under the MIT License.  
See the `LICENSE` file for details.

---