# 🛒 Cloud-Scale Retail Analytics Platform

**End-to-End Data Engineering Assignment (Bronze → Silver → Gold → Power BI)**

---

## 📌 Project Overview

This project implements an **enterprise-style cloud analytics platform** for a multinational retail company using **Apache Spark, Delta Lake, Azure Databricks (Community Edition), and Power BI**.

The objective is to design and build a **scalable, incremental, and analytics-ready data pipeline** starting from **raw data ingestion** to **executive dashboards**, following **industry-grade data engineering best practices**.

---

## 🧱 Architecture Overview

The solution follows the **Medallion Architecture**:

```
Raw CSV Files
     ↓
Bronze Layer (Raw Delta Tables)
     ↓
Silver Layer (Cleaned, Validated, Incremental)
     ↓
Gold Layer (Aggregated, Business-Ready)
     ↓
Power BI Dashboards
```

### Key Design Principles

* Incremental ingestion (no full reloads)
* Idempotent pipelines
* Data quality enforcement
* Delta Lake ACID guarantees
* Analytics-optimized Gold layer

---

## 📂 Repository Structure

```
6th Assignment/
│
├── code files/
│   ├── 01_bronze_sales_ingestion.ipynb
│   ├── 02_bronze_products_ingestion.ipynb
│   ├── 03_bronze_stores_ingestion.ipynb
│   ├── 04_silver_dimension_tables.ipynb
│   ├── 05_silver_sales_incremental.ipynb
│   └── 06_gold_aggregations.ipynb
│
├── data/
│   ├── sales/
│   │   ├── sales_day1.csv
│   │   ├── sales_day2.csv
│   │   ├── sales_day3.csv
|   |   ├── sales_day4.csv 
|   |   └── sales_day5.csv
│   │
│   ├── product/
│   │   └── product_master.csv
│   │
│   └── store/
│       └── store.csv
│
├── screenshots/
│   ├── executive_sales_overview.png
│   ├── product_performance.png
│   └── regional_store_performance.png
│
├── Cloud Sales Analytics.pbix
│
└── README.md
```

---

## 🗃️ Data Sources

### 1️⃣ Sales Transactions (Fact Data)

* Format: CSV
* Arrival: Daily (incremental)
* Directory: `data/sales/`

**Key Columns**

* transaction_id
* transaction_timestamp
* store_id
* product_id
* quantity
* unit_price
* discount
* total_amount
* currency

---

### 2️⃣ Product Master Data

* Format: CSV
* Directory: `data/product/`

**Key Columns**

* product_id
* product_name
* category
* brand
* standard_price

---

### 3️⃣ Store / Region Data

* Format: CSV
* Directory: `data/store/`

**Key Columns**

* store_id
* store_name
* region
* country

---

## 🟤 Bronze Layer — Raw Ingestion

**Purpose**

* Store raw, unmodified data
* Preserve auditability
* Capture ingestion metadata

**Features**

* Delta format storage
* Partitioned by ingestion date
* Metadata added:

  * `ingestion_timestamp`
  * `source_system`
  * `source_file`

---

## ⚪ Silver Layer — Cleaned & Standardized Data

**Purpose**

* Enforce data quality
* Apply business rules
* Support incremental processing

### Key Processing

* Deduplication using window functions
* Incremental loading using watermark logic
* Data calibration:

  ```
  total_amount = quantity × unit_price − discount
  ```
* Foreign key validation (product & store)
* Invalid records routed to quarantine table

### Silver Tables

* `silver_sales`
* `silver_sales_quarantine`
* `silver_products`
* `silver_stores`

---

## 🟡 Gold Layer — Analytics-Ready Data

Gold tables are **denormalized and aggregation-focused**, built only from Silver data.

### Gold Tables Created

| Table Name                  | Purpose                       |
| --------------------------- | ----------------------------- |
| `gold_daily_sales`          | Executive KPIs & daily trends |
| `gold_monthly_sales`        | Month-over-Month analysis     |
| `gold_monthly_region_sales` | Regional revenue trends       |
| `gold_product_performance`  | Top products & performance    |
| `gold_category_performance` | Category-wise analysis        |
| `gold_store_performance`    | Store & region drill-down     |

**Gold Layer Strategy**

* Overwrite mode (derived data)
* Optimized for Power BI
* Minimal DAX required

---

## 📊 Power BI Dashboards

### 1️⃣ Executive Sales Overview

* Total Revenue
* Total Transactions
* Total Quantity Sold
* Daily & Monthly Revenue Trends
* Revenue by Region

### 2️⃣ Product Performance

* Top Products by Revenue
* Category-wise Revenue
* Product Demand Trends

### 3️⃣ Regional & Store Performance

* Store-level KPIs
* Country & region comparison

Power BI connects **directly to Gold Delta tables** using the **Databricks connector**.

---

## 🛡️ Logging, Monitoring & Error Handling

### Logging Implemented

* Pipeline start & end time
* Records processed per run
* Records rejected (quarantine)
* Execution status (SUCCESS / FAILED)
* Error messages

All logs are stored in:

```
workspace.retail.pipeline_logs
```

### Error Handling

* Try–except wrapped pipelines
* Safe reruns supported
* No duplicate data on re-execution (MERGE logic)

---

## 🔁 Incremental & Rerun Safety

| Requirement           | Implementation                  |
| --------------------- | ------------------------------- |
| Incremental ingestion | Transaction timestamp watermark |
| Late-arriving data    | Sliding window logic            |
| Duplicate prevention  | Delta MERGE on business keys    |
| Rerun safety          | Idempotent pipelines            |

---
