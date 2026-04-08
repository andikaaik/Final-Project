# 📊 Data Pipeline Airflow Project

## 📌 Description
Project ini membangun data pipeline end-to-end menggunakan Apache Airflow untuk mengolah data penjualan dari CSV menjadi data warehouse.

---

## 🏗️ Architecture
Pipeline:
CSV → Staging → Data Warehouse → Analysis

---

## ⚙️ Tools
- Python
- Apache Airflow
- PostgreSQL
- Pandas

---

## 🔄 Pipeline Flow
1. Load data ke staging
2. Transform ke dimensi:
   - dim_time
   - dim_product
   - dim_customer
   - dim_employee
3. Load ke fact_sales

---

## 📊 Features
- ETL Automation
- Star Schema Data Warehouse
- Data Cleaning & Transformation

---

## 📁 Files
- `andika_pipeline.ipynb` → storytelling analysis
- `airflow_dag.py` → pipeline utama

---

## 👨‍💻 Author
Andika Indra.k