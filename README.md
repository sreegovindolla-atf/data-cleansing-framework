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

---

## Prerequisites

Install dependencies:
pip install -r requirements.txt




