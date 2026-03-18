# Data Warehouse Project

An end-to-end data warehouse built from scratch using a medallion architecture (Bronze → Silver → Gold). The project ingests raw CSV data from simulated CRM and ERP systems and processes it through structured layers toward an analytical data model.

SQL Server runs in a Docker container on Linux. SQL handles all data movement and transformation. Python handles orchestration, validation, and observability.

---

## Architecture

```
Source CSVs (CRM + ERP)
        │
        ▼
┌───────────────┐
│    Bronze     │  Raw ingestion — BULK INSERT, no transformation, NVARCHAR throughout
└───────┬───────┘
        │
        ▼
┌───────────────┐
│    Silver     │  Cleaned and conformed — type casting, deduplication, standardisation
└───────┬───────┘
        │
        ▼
┌───────────────┐
│     Gold      │  Analytical model — dimensional schema, business-ready views
└───────────────┘
```

Each layer follows the same pattern:

```
SQL child stored procs
        → SQL master orchestration proc
                → Python pipeline script
                        → Python validation script
```

A master Python orchestrator (in progress) will coordinate execution across all layers.

---

## Tech Stack

| Component        | Detail                                      |
|------------------|---------------------------------------------|
| Database         | SQL Server (Docker, Linux)                  |
| Orchestration    | Python                                      |
| Validation       | Python + pandas                             |
| Source data      | CSV files (CRM system, ERP system)          |
| Observability    | `admin.etl_run_log` table in SQL Server     |

---

## Repository Structure

```
sql/
├── init_database.sql              # Database and schema setup
├── admin_etl_run_log.sql          # ETL run log table provisioning
├── bronze/
│   ├── create_tables.sql          # Bronze table DDL (6 tables)
│   ├── cust_info_load.sql         # CRM customer info load proc
│   ├── prd_info_load.sql          # CRM product info load proc
│   ├── sales_details_load.sql     # CRM sales details load proc
│   ├── cust_az12_load.sql         # ERP customer load proc
│   ├── loc_a101_load.sql          # ERP location load proc
│   ├── px_cat_g1v2_load.sql       # ERP product category load proc
│   └── load_orchestration.sql     # Master proc: runs all bronze loads
├── silver/
│   ├── cust_info_transformation.sql     # CRM customer info transformation
│   ├── prd_info_transformation.sql      # CRM product info transformation
│   ├── sales_details_transformation.sql # CRM sales details transformation
│   └── cust_az12_transformation.sql     # ERP customer transformation
└── gold/                          # Planned

python/
├── config.py                      # Paths and connection config
├── cursor.py                      # Database connection helper
├── bronze_pipeline_orchestrator.py
├── data_validation.py
└── data_profiling.py
```

---

## Bronze Layer (Complete)

The bronze layer ingests raw CSV files directly into SQL Server with no transformation. All columns are stored as `NVARCHAR` with character-length buffers to accommodate dirty source data.

**Six tables across two source systems:**

| Table                    | Source |
|--------------------------|--------|
| `bronze.crm_cust_info`   | CRM    |
| `bronze.crm_prd_info`    | CRM    |
| `bronze.crm_sales_details` | CRM  |
| `bronze.erp_cust_az12`   | ERP    |
| `bronze.erp_loc_a101`    | ERP    |
| `bronze.erp_px_cat_g1v2` | ERP    |

**Pipeline design:**
- Each table has a dedicated stored procedure using `BULK INSERT`
- A master procedure (`bronze.load_bronze_all`) orchestrates all six loads in sequence with structured `TRY/CATCH` error handling
- Failures are classified by error type: `INFRASTRUCTURE`, `INGESTION`, `DATA QUALITY`, `CODE`, or `OTHER`
- Every execution is logged to `admin.etl_run_log` with status, timing, and error detail

**Observability (`admin.etl_run_log`):**

| Column               | Purpose                                    |
|----------------------|--------------------------------------------|
| `run_id`             | UUID grouping all steps in a single run    |
| `proc_name`          | Stored procedure that executed             |
| `run_status`         | `STARTED` → `SUCCESS` or `FAILED`         |
| `layer`              | Pipeline layer (`bronze`, `silver`, etc.)  |
| `error_class`        | Categorised failure type                   |
| `error_message`      | SQL error detail on failure                |
| `rows_read`          | Row count from source CSV (set by Python)  |
| `rows_written`       | Row count loaded to table (set by Python)  |
| `validation_status`  | `SUCCESS` or `FAILED` (set by Python)      |
| `query_run_time_ms`  | Execution duration in milliseconds         |

**Python layer:**
- `bronze_pipeline_orchestrator.py` — executes the SQL master proc, reads post-run log output, and raises a custom `BronzePipelineFailed` exception on failure
- `data_validation.py` — compares row counts between source CSVs and bronze tables; on mismatch, marks affected tables as `FAILED` in the run log and raises `RowMismatch` or `KeyMismatch`

---

## Silver Layer (In Progress)

The silver layer cleans and standardises raw bronze data. Transformations are applied in staged CTEs, each with a single clearly-named responsibility. Silver does not impute missing values or apply business-rule-based enrichment — those decisions are deferred to Gold.

**CRM tables complete:**

| Table                      | Key transformations                                                                 |
|----------------------------|-------------------------------------------------------------------------------------|
| `crm_cust_info`            | Type casting, whitespace trimming, duplicate resolution (fewest nulls → latest date) |
| `crm_prd_info`             | Key splitting, product line expansion, end date reconstruction from next start date  |
| `crm_sales_details`        | Date format conversion, financial field derivation and reconciliation, bad data flagging |

**ERP tables in progress:**

| Table                      | Key transformations                                                                 |
|----------------------------|-------------------------------------------------------------------------------------|
| `erp_cust_az12`            | Customer ID normalisation to cst_key format, date casting, gender standardisation   |

Remaining ERP tables and the Python pipeline/validation layer are planned.

---

## Gold Layer (Planned)

The gold layer will expose a dimensional model optimised for analytical queries:

- Dimension tables: customers, products, locations
- Fact table: sales transactions
- Business-ready views for reporting

**Known items deferred from Silver:**

| Item | Source table | Detail |
|------|-------------|--------|
| NULL order date imputation | `crm_sales_details` | Where `sls_order_dt` is NULL, attempt to borrow the date from another row with the same order number; if no sibling exists, leave as NULL |
| Negative sales classification | `crm_sales_details` | Determine whether negative `sls_sales` values represent legitimate returns or data errors; requires a defined return window business rule |
| Incomplete financial data | `crm_sales_details` | Rows flagged `sls_bad_financial_data = 'Y'` (both `sls_sales` and `sls_price` are NULL) need a resolution strategy |
| Date chronological validation | `crm_sales_details` | Enforce `sls_order_dt <= sls_ship_dt <= sls_due_dt`; rows violating this require a business rule to determine which date is incorrect |
| Unrecognised coded field values | All Silver tables | In CASE WHEN transformations that map source codes to readable labels (e.g. gender, product line), values outside the known set are set to NULL in Silver. Gold will apply a business rule to label these — for example as `'Unknown'` or a catch-all category — once business context is available |

---

## Design Assumptions

**Bronze — raw ingestion**
- Source data is assumed to be dirty. No transformation is applied at this layer; all fields are stored as `NVARCHAR` to absorb type inconsistencies without rejecting rows. Type casting is deliberately deferred to silver.
- `BULK INSERT` is run as a truncate-and-reload. Row count is therefore sufficient for validation — if the load fails mid-way, the table is empty and the count mismatch surfaces the problem.
- One customer record contains an accented character (é in "Andrés") that is garbled on load. `BULK INSERT` on SQL Server for Linux does not support the `CODEPAGE` parameter, so the encoding of the source file cannot be specified. The practical workarounds — converting individual files or all files to UTF-16 — either introduce inconsistency between source files or require changes across all load procedures, neither of which is considered good practice. This is accepted as a known limitation of running SQL Server on Linux. In production, this would be handled by a cloud-native ingestion tool or a Windows-hosted SQL Server instance where `CODEPAGE` is supported.

**Pipeline architecture — stored procedures over scripts**
- Stored procedures were chosen over plain SQL scripts to keep orchestration logic at the database level. The master proc owns sequencing, error handling, and run logging, so Python only needs a single call to execute the full layer.
- Writing to the run log from within the `TRY/CATCH` block is straightforward when the logic lives in SQL Server — doing the same from Python would mean catching SQL errors and issuing separate logging queries across two languages.
- Child procs are reusable database objects that can be called from anywhere, and adding a new table requires only a new child proc and two lines in the master proc with no changes to Python.

**Silver — cleaning boundary**
- Silver is responsible for cleaning and standardising only: format corrections, type casting, and resolving errors that can be derived mathematically. It does not impute missing values or apply business rules to interpret data intent. Those decisions belong in Gold, where business context is available.

**CRM and ERP integration**
- The join strategy between CRM and ERP tables is being determined through data profiling. Each table is being analysed individually before cross-system relationships are defined.

---

## Data Sources

Raw CSV files are not version-controlled. The bronze table definitions act as the schema contract for the expected structure of source data. Source files are mounted into the Docker container and accessed via `BULK INSERT`.
