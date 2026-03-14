# Real-Time Orders Validation & Customer Analytics Pipeline
### Azure Data Factory · Databricks · ADLS Gen2 · Azure Key Vault · Azure SQL DB · PySpark · Delta Lake

---

## About this project

This is a personal project I built in my own time while upskilling toward the Databricks Spark Developer certification. Rather than just doing cert prep, I wanted to build something end-to-end on Azure that reflects how I'd approach a real ingestion problem — event-driven triggering, cross-cloud data movement, proper secret management, and an audit trail. The kind of things that don't show up in certification prep but matter in production.

The dataset is a retail orders domain (68,882 orders, 12,435 customers) which gave enough real-world messiness to surface an actual bug during testing — a `CANCELED` vs `CANCELLED` spelling mismatch that would have silently discarded ~2% of valid orders on every pipeline run. More on that below.

---

## Business Problem

A retail e-commerce business receives order files from a third-party fulfilment service at unpredictable intervals. Before this data feeds downstream BI dashboards and customer analytics, two data quality checks must pass:

1. No duplicate `order_id` values — duplicates inflate revenue figures and corrupt SLA reporting
2. Only recognised order statuses — invalid values break dashboard filters and customer communication workflows

Beyond validation, the business needs a single consolidated view: how many orders has each customer placed, and what is their total spend? That answer is spread across three different source systems — orders in ADLS Gen2, order line items in Amazon S3 (JSON), and customer master data in Azure SQL DB. The pipeline joins all three automatically after validation passes.

If any check fails, the file is quarantined with a machine-readable error report — no manual intervention needed.

---

## Architecture

```
┌─────────────────────┐
│  Third Party Service │
│  (orders.csv)        │
└──────────┬──────────┘
           │ file drop
           ▼
┌─────────────────────┐         Storage Event          ┌────────────────────────┐
│     ADLS Gen2        │ ──────── Trigger ────────────▶ │   Azure Data Factory   │
│     /landing         │                                │   (Orchestration)      │
└─────────────────────┘                                └───────────┬────────────┘
                                                                   │
                                              ┌────────────────────┼────────────────────┐
                                              │                    │                    │
                                    ┌─────────▼──────┐  ┌─────────▼──────┐  ┌──────────▼──────┐
                                    │  Copy Activity  │  │   Databricks   │  │  Amazon S3      │
                                    │  S3 → ADLS Gen2 │  │   Notebook     │  │  order_items    │
                                    │  (JSON → CSV)   │  │   (PySpark)    │  │  (JSON)         │
                                    └────────────────┘  └────────┬───────┘  └─────────────────┘
                                                                  │
                                   ┌──────────────────────────────┼──────────────────────────┐
                                   │                              │                          │
                         ┌─────────▼──────┐            ┌─────────▼──────┐        ┌──────────▼──────┐
                         │  ADLS Gen2      │            │  Azure SQL DB   │        │  ADLS Gen2      │
                         │  /staging       │            │  Audit Log +    │        │  /discarded     │
                         │  (clean files)  │            │  Results Table  │        │  + Quality Rpt  │
                         └────────────────┘            └────────────────┘        └─────────────────┘
```

Secrets management: Azure Key Vault stores the storage account key and SQL password. Databricks accesses credentials via a Key Vault-backed Secret Scope — no credentials exist in notebooks, ADF, or source control.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Orchestration | Azure Data Factory | Storage Event Trigger, Copy Activity, pipeline parameterisation |
| Compute | Azure Databricks (PySpark) | Validation logic, joins, aggregations |
| Storage | ADLS Gen2 (Hierarchical Namespace) | Landing, staging, discarded zones |
| Secret Management | Azure Key Vault + Databricks Secret Scope | Zero secrets in code |
| Reference / Output | Azure SQL Database | Lookup table, audit log, results |
| Source (external) | Amazon S3 | order_items JSON ingested via ADF |
| File Format | CSV, JSON, Delta | Multi-format ingestion |

---

## Dataset

| Dataset | Source | Format | Scale |
|---|---|---|---|
| orders | ADLS Gen2 /landing | CSV | 68,882 rows |
| order_items | Amazon S3 | JSON → CSV via ADF | Multi-line JSON |
| customers | Azure SQL DB (loaded via pipeline2) | CSV → SQL | 12,435 rows |

Real datasets used throughout — not synthetic toy data. Orders span 9 status types across a multi-year transaction history.

---

## Pipeline 1 — Orders Validation + Analytics

### Trigger
Storage Event Trigger fires when any file lands in `/landing`. Filename is captured dynamically via `@triggerBody().filename` — no hardcoded filenames anywhere in the pipeline.

### Step 1 — Fetch order_items from S3
Copy Activity pulls `order_items.json` from Amazon S3, converts JSON to CSV with explicit field-level schema mapping, and lands it in ADLS Gen2 before the notebook runs.

### Step 2 — Databricks Notebook (PySpark)

Check 0 — Idempotent Mount
```python
for x in dbutils.fs.mounts():
    if x.mountPoint == '/mnt/sales':
        alreadyMounted = True
        break
```
Mount check runs before every execution — prevents cluster restart failures without adding complexity.

Check 1 — Schema Validation
Validates incoming column names against an expected schema before any processing. Catches upstream structural changes from the third party early, before they surface as cryptic Spark errors downstream.

Check 2 — Duplicate Order ID Detection
```python
ordersCount         = ordersDF.count()
distinctOrdersCount = ordersDF.select('order_id').distinct().count()
```
Any mismatch → file quarantined, audit record written, notebook exits with structured JSON error message.

Check 3 — Valid Order Status (SQL DB Lookup)
Valid statuses are maintained in `dbo.valid_order_status` in Azure SQL DB. The notebook reads this table via JDBC at runtime. When the business adds or removes valid statuses, they update the table directly — no code change, no redeployment needed.

```python
validStatusDF = spark.read.jdbc(url=connectionUrl, table='dbo.valid_order_status', ...)
```

Analytics Join
On passing all checks, three data sources are joined in Spark:

```sql
SELECT
    c.customer_id, c.customer_fname, c.customer_lname,
    c.customer_city, c.customer_state,
    COUNT(DISTINCT o.order_id)            AS num_orders_placed,
    ROUND(SUM(oi.order_item_subtotal), 2) AS total_amount_spent
FROM customers c
JOIN orders      o  ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id    = oi.order_item_order_id
GROUP BY c.customer_id, c.customer_fname, c.customer_lname,
         c.customer_city, c.customer_state
ORDER BY total_amount_spent DESC
```

Results written to `dbo.customer_order_summary` in Azure SQL DB for BI consumption.

---

## Pipeline 2 — Customer Master Data Load

Separate pipeline loading `customers.csv` from ADLS Gen2 into `dbo.customers` in Azure SQL DB via ADF Copy Activity. The schema mapping in ADF handles column renames between source and sink (`username → customer_email`, `address → customer_street`, `pincode → customer_zipcode`) and type conversion (`String → INT` for `customer_id`) — no transformation code needed.

---

## Enhancements over a basic implementation

| Enhancement | Implementation | Why it matters |
|---|---|---|
| Schema validation | Column set check before processing | Catches third-party structural changes early |
| Idempotent mount | `dbutils.fs.mounts()` check | Prevents warm-cluster restart failures |
| Delta audit log | Appended on every run to `/mnt/sales/audit` | Full lineage — filename, timestamp, row count, pass/fail, error |
| Data quality report | Invalid rows written to `/discarded/quality_report_*` | Ops team can see exactly which rows failed without touching Databricks |
| Structured notebook exit | JSON exit message with errorFlg + rowsProcessed | ADF can parse exit status for downstream branching |
| Dynamic filename | `@triggerBody().filename` passed as widget | Pipeline handles any filename — not brittle to renames |
| Zero secrets in code | All credentials via Key Vault Secret Scope | Safe to open-source, meets security compliance |

---

## Security Design

```
Azure Data Factory
      │
      │  Access Policy (Managed Identity)
      ▼
Azure Key Vault
  ├── storage-account-key  ──▶  used in ADF linked service (ADLS Gen2)
  └── sql-password         ──▶  used in Databricks via Secret Scope
           │
           │  dbutils.secrets.get(scope, key)
           ▼
     Databricks Notebook
```

3 Linked Services: ADLS Gen2 · Databricks · Key Vault
Databricks Secret Scope backed by Key Vault — token never stored in notebook.

---

## The CANCELED vs CANCELLED bug

During testing with the real dataset, the status `CANCELED` (one L) in the orders data did not match `CANCELLED` (two L's) in the initial lookup table. That means 1,428 valid orders — about 2% of all records — would have been silently routed to `/discarded` on every pipeline run. It would have gone unnoticed until a finance report came up short weeks later.

Fixed by checking the actual distinct values in the real dataset before seeding the lookup table, rather than copying status names from documentation. Small thing, but it's the kind of issue that only surfaces when you test with production-representative data.

---

## Repo Structure

```
orders-validation-pipeline/
│
├── README.md
├── notebooks/
│   └── orders_validation.py          # PySpark validation + join logic
├── adf_pipeline/
│   ├── pipeline1_definition.json     # S3 Copy + Databricks notebook trigger
│   └── pipeline2_definition.json     # Customer CSV → Azure SQL DB load
├── sql/
│   └── all_tables.sql                # DDL for all 4 tables + seed data
├── sample_data/
│   ├── orders.csv                    # 68,882 real order records
│   ├── customers.csv                 # 12,435 real customer records
│   ├── week24_orders_test.csv        # Small test fixture (4 rows)
│   ├── orders_invalid_duplicate.csv  # Triggers duplicate check failure
│   ├── orders_invalid_status.csv     # Triggers status check failure
│   └── orders_invalid_schema.csv     # Triggers schema check failure
└── .gitignore
```

---

## How to Deploy

1. ADLS Gen2 — Create storage account with hierarchical namespace. Container `sales` with directories: `landing/`, `staging/`, `discarded/`, `audit/`
2. Azure Key Vault — Add secrets: `storage-account-key`, `sql-password`
3. Azure SQL DB — Run `sql/all_tables.sql`. Enable Azure Services access in firewall settings
4. Databricks — Create workspace. Navigate to `<workspace-url>#secrets/createScope` to create a Key Vault-backed secret scope named `databricksscope`
5. ADF — Create Linked Services for ADLS Gen2, Databricks, Key Vault, and Amazon S3. Import the pipeline JSON files. Configure Storage Event Trigger on `sales` container, path begins with `landing/`
6. Test — Upload each file from `sample_data/` to `/landing` and observe pipeline behaviour in the ADF Monitor tab

---

## Author

Saksham Jain — Databricks Certified Data Engineer Associate
3 years experience · Azure · Databricks · PySpark · Delta Lake · Medallion Architecture

[![LinkedIn](https://img.shields.io/badge/LinkedIn-sakshamjain8-blue)](https://www.linkedin.com/in/sakshamjain8)
[![Email](https://img.shields.io/badge/Email-sakshamjain.2008%40gmail.com-red)](mailto:sakshamjain.2008@gmail.com)
