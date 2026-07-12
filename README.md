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

---

## 📊 Business Questions Solutions

Below are the optimized, production-ready SQL queries written to solve the technical assessment's business questions.

### Q1. Top 5 Products by Total Revenue in 2025
```sql
SELECT 
    p.product_id,
    p.product_name,
    SUM(od.quantity * od.unit_price) AS total_revenue
FROM orders o
JOIN order_details od ON o.order_id = od.order_id
JOIN products p ON od.product_id = p.product_id
WHERE o.order_date >= '2025-01-01' AND o.order_date <= '2025-12-31'
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC
LIMIT 5;

---

WITH MonthlyRegionRevenue AS (
    SELECT 
        r.region_id,
        r.region_name,
        EXTRACT(MONTH FROM o.order_date) AS order_month,
        SUM(od.quantity * od.unit_price) AS current_month_revenue
    FROM orders o
    JOIN order_details od ON o.order_id = od.order_id
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN regions r ON c.region_id = r.region_id
    WHERE o.order_date >= '2025-07-01' AND o.order_date <= '2025-09-30'
    GROUP BY r.region_id, r.region_name, EXTRACT(MONTH FROM o.order_date)
),
MoMGrowth AS (
    SELECT 
        region_id,
        region_name,
        order_month,
        current_month_revenue,
        LAG(current_month_revenue) OVER (PARTITION BY region_id ORDER BY order_month) AS previous_month_revenue,
        ((current_month_revenue - LAG(current_month_revenue) OVER (PARTITION BY region_id ORDER BY order_month)) 
        / LAG(current_month_revenue) OVER (PARTITION BY region_id ORDER BY order_month)) * 100 AS mom_growth_pct
    FROM MonthlyRegionRevenue
)
SELECT 
    region_id,
    region_name,
    order_month,
    mom_growth_pct
FROM MoMGrowth
WHERE previous_month_revenue IS NOT NULL
ORDER BY mom_growth_pct DESC
LIMIT 1;

---

SELECT 
    s.salesperson_id,
    s.salesperson_name,
    AVG((m.actual_sales / m.target_sales) * 100) AS avg_target_achievement_pct
FROM salespeople s
JOIN monthly_targets_actuals m ON s.salesperson_id = m.salesperson_id
WHERE m.target_sales > 0
GROUP BY s.salesperson_id, s.salesperson_name
ORDER BY avg_target_achievement_pct DESC;

---

SELECT 
    d.distributor_id,
    d.distributor_name,
    COUNT(t.transaction_id) AS total_transactions,
    COUNT(CASE WHEN t.status = 'Returned' THEN 1 END) AS returned_transactions,
    (COUNT(CASE WHEN t.status = 'Returned' THEN 1 END) * 1.0 / COUNT(t.transaction_id)) AS return_rate
FROM distributors d
JOIN transactions t ON d.distributor_id = t.distributor_id
GROUP BY d.distributor_id, d.distributor_name
HAVING COUNT(t.transaction_id) > 0
ORDER BY return_rate DESC
LIMIT 1;

---

WITH MonthlyCategoryRevenue AS (
    SELECT 
        p.category_id,
        p.category_name,
        DATE_TRUNC('month', o.order_date) AS order_month,
        SUM(od.quantity * od.unit_price) AS monthly_revenue
    FROM orders o
    JOIN order_details od ON o.order_id = od.order_id
    JOIN products p ON od.product_id = p.product_id
    GROUP BY p.category_id, p.category_name, DATE_TRUNC('month', o.order_date)
)
SELECT 
    category_id,
    category_name,
    order_month,
    monthly_revenue,
    SUM(monthly_revenue) OVER (
        PARTITION BY category_id 
        ORDER BY order_month 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3_month_revenue
FROM MonthlyCategoryRevenue
ORDER BY category_name, order_month;