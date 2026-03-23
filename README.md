# Databricks ELT Project

## 📌 Overview
This project implements an end-to-end ELT (Extract, Load, Transform) pipeline on Databricks using a medallion architecture (Bronze, Silver, Gold).  

The pipeline ingests both batch and simulated streaming data, processes it incrementally, and produces analytical datasets for business intelligence and reporting.

The primary goal is to demonstrate how modern data platforms can support scalable, automated, and maintainable data pipelines.

---

## 🏗️ Architecture

The project follows the Medallion Architecture pattern:

### 🔹 Bronze Layer (Raw Ingestion)
- Ingests raw data from multiple sources:
  - CSV datasets (batch ingestion)
  - Simulated web events and ads data (mock streaming)
- Uses Auto Loader for incremental ingestion
- Stores data in raw format with minimal transformation

### 🔹 Silver Layer (Data Cleaning & Transformation)
- Cleans and standardizes raw data
- Handles null values, type casting, and data enrichment
- Joins multiple datasets into structured tables
- Prepares data for analytics

### 🔹 Gold Layer (Business & Analytics)
- Builds fact and dimension tables
- Creates aggregated data marts for reporting
- Supports business use cases such as:
  - Sales analysis
  - Marketing performance tracking
  - Funnel conversion analysis

---

## ⚙️ Data Pipeline & Orchestration

- Pipelines are orchestrated using Databricks Workflows
- Jobs are defined and deployed via Databricks Asset Bundles (Infrastructure as Code)
- Task dependencies:
  - Bronze ingestion (parallel)
  - Silver transformation (depends on Bronze)
  - Gold layer (depends on Silver)
- Supports incremental processing

---

## 📊 Data Model

### Fact Tables
- `fact_orders`
- `fact_order_items`
- `fact_marketing_spend_daily`
- `fact_web_funnel_daily`
- `fact_order_fulfillment`

### Dimension Tables
- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_date`
- `dim_geolocation`
- `dim_campaign`
- `dim_channel`

### Data Mart
- `mart_sales_marketing_360`
- `mart_kpi_daily`

---

## 🎯 Use Cases

This dataset supports multiple analytical scenarios:

- Marketing performance analysis (CTR, CPC, conversions)
- Customer behavior and funnel analysis
- Sales and revenue reporting
- Delivery performance and logistics insights

---

## 🧪 Data Sources

- Public e-commerce dataset (Olist)
- Simulated data:
  - Web events (user activity)
  - Ads data (campaign performance)

---

## 🛠️ Tech Stack

- Databricks (Lakehouse platform)
- PySpark (data processing)
- Delta Lake (storage layer)
- Databricks Workflows (orchestration)
- Databricks Asset Bundles (deployment & IaC)
- GitHub (version control)

---

## 🚀 Key Features

- Medallion architecture implementation
- Incremental data ingestion with Auto Loader
- Modular and scalable pipeline design
- Job orchestration with dependency management
- Version-controlled deployment using GitHub + Bundles

---

## 📈 Notes

This project focuses on building a Data Warehouse (DW) pipeline.  
Dashboards are used only for demonstration purposes to validate the data model and transformations.