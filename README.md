# FMCG Sales Operations End-to-End Data Pipeline

An automated, containerized ELT pipeline designed to ingest, clean, and transform Fast-Moving Consumer Goods (FMCG) sales operations data for downstream analytical reporting.

---

## 🚀 Architecture Overview

The system architecture is completely containerized and modular, separating orchestration, ingestion, and analytical transformation layers:

*   **Orchestration:** Apache Airflow manages workflow task dependencies as a Directed Acyclic Graph (DAG).
*   **Ingestion:** Custom Python extraction scripts pull raw sales operational data and enforce target schemas during loading.
*   **Storage & Warehouse:** PostgreSQL handles both the raw staging layer and the structured analytics layer.
*   **Transformations:** dbt (Data Build Tool) builds version-controlled, modular data models and manages automated testing.
*   **Containerization:** Docker & Docker Compose ensure complete environment isolation and instant local deployment.

---

## 🛠️ Local Setup & Deployment

Ensure you have **Docker Desktop** installed on your system before running the pipeline.

### 1. Initialize the Stack
From the root directory of the project, execute the following command to build and launch all services:
```bash
docker compose up --build -d