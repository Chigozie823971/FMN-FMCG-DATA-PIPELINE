# FMCG Sales Operations End-to-End Data Pipeline

An automated, containerized ELT pipeline designed to ingest, clean, and transform Fast-Moving Consumer Goods (FMCG) sales operations data for downstream analytical reporting.

---

## 🚀 Architecture Decisions

The system architecture is completely containerized and modular, separating orchestration, ingestion, and analytical transformation layers:

*   **Orchestration:** Apache Airflow manages workflow task dependencies as a Directed Acyclic Graph (DAG) to ensure reliable execution.
*   **Ingestion:** Custom Python extraction scripts pull raw sales operational data from the source XLSX files and enforce target schemas during loading.
*   **Storage & Warehouse:** PostgreSQL handles both the raw staging layer and the structured analytics layer.
*   **Transformations:** dbt (Data Build Tool) builds version-controlled, modular data models and manages transformations.
*   **Containerization:** Docker & Docker Compose ensure complete environment isolation and instant local deployment.

---

## 📐 Schema Design & Pipeline Logic

The database is structured to support robust relational data integrity:
*   **Product Table:** Houses the unique catalog of items. The `product_id` serves as the **Primary Key**.
*   **Sales Table:** Tracks historical transactional operations. The `product_id` acts as a **Foreign Key**, establishing a reliable one-to-many relationship from products to sales.

### Pipeline Workflow:
1. **Extract & Load (EL):** Airflow triggers a Python task that reads the source `.xlsx` file, maps data types, and loads the raw data into a PostgreSQL staging schema.
2. **Transform (T):** Airflow triggers dbt to run modular SQL models, transforming raw operational data into structured, analytics-ready tables.

---

## 📊 Data Quality & Testing

Data integrity is protected at two distinct checkpoints:
1.  **Ingestion Validation:** The Python scripts validate column structures and handle data type constraints upon raw database writes.
2.  **dbt Automated Test Assertions:** Automated tests run on critical constraints (`not_null` and `unique` assertions on primary keys) to guarantee data quality and prevent analytical data corruption.

---

## 🛠️ Local Setup & Deployment Instructions

Ensure you have **Docker Desktop** installed on your system before running the pipeline.

### 1. Initialize the Stack
From the root directory of the project, execute the following command to build and launch all services:
```bash
docker compose up --build -d