# E-Commerce Data Analytics Platform

> **Azure Data Engineering | Medallion Architecture | ADF · ADLS Gen2 · Azure Databricks · Microsoft Fabric · Delta Lake · Azure Purview · Azure Key Vault**

---

## Problem Statement

A US-based e-commerce client had data split across three systems — an Order Management System, a Product Inventory System, and a CRM. Each team pulled data independently through manual Excel exports. Reports contradicted each other, took **2–3 days to produce**, and there was no single source of truth. The business needed one unified platform where sales, inventory, and customer data could be analysed together with historical tracking.

---

## Architecture Overview

```
[OMS REST API]       ─┐
[Inventory SFTP]     ─┤──► ADF (Parameterized, Watermark-based)
[CRM Azure SQL]      ─┤         │
[Product Cat SQL]    ─┘         ▼
                          ADLS Gen2 Bronze (Delta, append-only)
                                 │
                          Azure Databricks (PySpark Notebooks)
                                 │
                          ADLS Gen2 Silver (Delta, partitioned by load_date)
                                 │
                          Azure Databricks (PySpark Notebooks)
                                 │
                          Microsoft Fabric Warehouse (Gold — Star Schema)
                                 │
                          Power BI (DirectQuery) ──► 3 Dashboards
                                 │
                    Azure Purview (Governance) + Azure Key Vault (Secrets)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Azure Data Factory (ADF) |
| Storage | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Transformation | Azure Databricks (PySpark) |
| Serving | Microsoft Fabric Warehouse |
| Table Format | Delta Lake |
| Governance | Azure Purview |
| Secrets Management | Azure Key Vault |
| Reporting | Power BI (DirectQuery) |
| Version Control | GitHub |

---

## Data Flow — Medallion Architecture

### 🥉 Bronze Layer — Raw Ingestion

All 4 sources ingested via **ADF**, nightly at 11 PM IST (US morning), using a **single parameterized pipeline** driven by a control table — adding a new source required only a config row, not a new pipeline.

| Source | Method | Frequency | ADF Connector |
|---|---|---|---|
| Order Management System | REST API | Daily | HTTP + Watermark on `order_updated_at` |
| Inventory System | SFTP flat files | Daily | SFTP Connector |
| CRM | Azure SQL | Daily | Azure SQL Connector |
| Product Catalogue | Azure SQL | Weekly | Azure SQL Connector |

- All records landed in **ADLS Gen2 Bronze** as **Delta files**, append-only
- Audit columns added to every record: `ingestion_timestamp`, `source_system`, `pipeline_run_id`
- No transformations at this layer — raw as received

### 🥈 Silver Layer — Cleanse & Enrich

ADF triggered a **Databricks Notebook Activity** post-ingestion. PySpark notebooks handled the full cleaning and enrichment layer:

- **Deduplication** via `ROW_NUMBER()` on business keys (order events had API duplicates)
- **Date standardisation**, currency conversion to USD, null handling
- **Schema validation** — records missing mandatory fields (e.g., `order_id`) routed to quarantine table with alert; no bad data passed silently
- **Product Catalogue join** at Silver — enriched each order line with category and unit price so Power BI never computed joins at query time
- **SCD Type 2** on Customer dimension via **Delta MERGE** — tracked city and loyalty tier changes with `end_date` and `is_current` flag
- **SCD Type 1** on Product dimension — simple overwrite for attribute corrections
- Output written to **ADLS Gen2 Silver**, partitioned by `load_date`

### 🥇 Gold Layer — Aggregate & Serve

Second set of Databricks notebooks built the **star schema** loaded into **Fabric Warehouse**:

**Fact Tables:** `fact_sales`, `fact_inventory`

**Dimension Tables:** `dim_customer`, `dim_product`, `dim_date`, `dim_warehouse`

**Pre-aggregated KPI Tables:**
- Monthly revenue by category
- Customer lifetime value buckets
- Inventory turnover

After every Gold load: **`OPTIMIZE` + `ZORDER BY`** on Delta tables. `fact_sales` ZORDERed by `order_date` and `product_id` — the two most common `WHERE` filters in reports. This physically co-locates related data on disk, directly causing the **40% query performance improvement**.

---

## Governance & Security

- **Azure Purview** scanned all three layers and classified PII columns across the platform
- **Azure Key Vault** stored all credentials — nothing hardcoded in notebooks or pipelines
- **Access control:** Analysts → Gold only | Engineers → Silver | Admin → Bronze
- **ADF Monitor** + **Teams alerts** on pipeline failures; Monitoring Hub reviewed every morning before standup

---

## Key Outcomes

| Metric | Before | After |
|---|---|---|
| Report turnaround | 2–3 days | Same day |
| Pipeline execution time | Baseline | **-30%** |
| Query performance | Baseline | **+40%** (Delta ZORDER) |
| Data quality issues | Untracked | **-30%** |
| Dev effort for new sources | New pipeline build | **-40%** (metadata-driven) |

---

## Repository Structure

```
ecommerce-azure-pipeline/
├── README.md
├── adf_pipelines/
│   ├── pipeline_master_ingestion.json       # Parameterized ADF pipeline export
│   └── linked_services_template.json        # Sanitized linked service templates
├── databricks_notebooks/
│   ├── bronze_ingestion_audit.py            # Audit column injection logic
│   ├── silver_cleanse_enrich.py             # Dedup, standardise, SCD logic
│   ├── silver_scd_type2_customer.py         # Delta MERGE SCD Type 2
│   └── gold_star_schema_load.py             # Fact/dim load + OPTIMIZE/ZORDER
├── sql/
│   ├── fabric_warehouse_ddl.sql             # Fact and dimension table DDL
│   ├── gold_kpi_aggregates.sql              # Pre-aggregated KPI views
│   └── delta_zorder_optimization.sql        # OPTIMIZE + ZORDER statements
├── config/
│   └── control_table_schema.sql             # Metadata-driven pipeline config table
└── docs/
    └── architecture_diagram.png
```

---

## Author

**Abhishek Kamble** | Azure Data Engineer | Microsoft DP-700 Certified  
📧 abhishekbkamble32@gmail.com | 📍 Pune, India  
[LinkedIn](#) · [GitHub](#)
