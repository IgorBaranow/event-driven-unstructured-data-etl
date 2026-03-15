# Reactive Enterprise Data Ingestion Pipeline (ETL)

## 📌 Overview
An automated end-to-end ETL pipeline designed to ingest, normalize, and store unstructured data from external vendors. The system monitors incoming communication streams in real-time, extracts multi-format attachments (PDF, Excel, CSV), and persists cleaned data into a centralized Data Warehouse.

**Business Value:** Eliminated ~40 man-hours/month of manual data entry by automating the ingestion of complex logistics documentation.

## 🛠 Tech Stack
- **Language:** Python 3.10+
- **Data Libraries:** Pandas (Data manipulation), PDFPlumber (Structural PDF extraction), OpenPyXL (Excel processing).
- **Storage:** SQLite (Enterprise Data Warehouse mockup) with UPSERT logic.
- **Integration:** Win32COM (Microsoft Outlook Integration).
- **Architecture:** Object-Oriented Programming (OOP) with Abstract Base Classes.

## 🏗 System Architecture
The project follows a modular "Strategy" pattern to ensure high scalability:

1. **Ingestion Gateway (`main.py` & `event_handler.py`):** Real-time monitoring of data streams.
2. **Rule Engine (`rules.py`):** Metadata-based filtering logic to route only relevant payloads.
3. **Dynamic Dispatcher (`convert_dispatcher.py`):** Routes payloads to specific vendor-logic plugins.
4. **ETL Core (`base_converter.py`):** Abstract layer enforcing standardized processing.
5. **Persistence Layer (`db_utils.py`):** Advanced SQL merging (Staging -> Production) to ensure idempotency.

## 🚀 Key Features
- **Dynamic Header Detection:** Excel parsers identify data tables by keyword search rather than hardcoded coordinates.
- **Structural PDF Extraction:** Extracts nested tables directly from PDF frames into DataFrames.
- **Data Sanitization:** A robust regex-based engine to handle over 12+ international date formats.
- **Idempotent Ingestion:** Prevents data duplication using a custom Staging-to-Upsert SQL pattern.

## 📂 Project Structure
(Paste your folder tree here)



All components are interconnected in a single, automated pipeline:

main.py – Launches a persistent loop to monitor the Outlook inbox in real-time.

event_handler.py & rules.py – As soon as an email arrives, the system validates it against predefined ingestion criteria.

convert_utils.py – If the email is matched, the system automatically downloads all attachments to a secure temporary folder.

convert_dispatcher.py – Identifies the specific vendor based on the source and routes the file to the correct processing class.

vendor_a/b/c.py (inheriting from base_converter.py) – Executes specialized parsing logic to extract data from PDF or Excel files.

db_utils.py & date_utils.py – Cleans the extracted data (standardizing formats) and performs an UPSERT into the database to ensure no duplicate entries.