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
├── silver/                        # In progress
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

The silver layer will clean and conform raw bronze data. Work begun on `crm_cust_info`:

- Type casting (e.g. `NVARCHAR` → `INT`, `DATE`)
- Whitespace trimming
- Duplicate resolution using a multi-step strategy: prefer records with fewer nulls, then latest `cst_create_date`

Remaining tables and the Python pipeline/validation layer are planned.

---

## Gold Layer (Planned)

The gold layer will expose a dimensional model optimised for analytical queries:

- Dimension tables: customers, products, locations
- Fact table: sales transactions
- Business-ready views for reporting

---

## Design Assumptions

**Bronze — raw ingestion**
- Source data is assumed to be dirty. No transformation is applied at this layer; all fields are stored as `NVARCHAR` to absorb type inconsistencies without rejecting rows. Type casting is deliberately deferred to silver.
- `BULK INSERT` is run as a truncate-and-reload. Row count is therefore sufficient for validation — if the load fails mid-way, the table is empty and the count mismatch surfaces the problem.

**Silver — deduplication**
- Duplicate rows in the CRM customer table are assumed to represent updated versions of a record, not genuine duplicates. The source system does not flag which version is current, so a resolution strategy is applied: prefer the record with fewer null values across descriptive fields, then prefer the latest `cst_create_date` as a tie-breaker.
- The null count covers all descriptive columns, excluding the row identifier and the date field (which serves a separate role in the tie-breaking logic).
- Known gap: if two versions of the same customer have an identical null count *and* the same `cst_create_date`, both rows are returned. This is accepted as a rare edge case and is out of scope for this project — handling it would require additional business rules or a source-side record version flag that does not exist in the data.

**Pipeline architecture — stored procedures over scripts**
- Stored procedures were chosen over plain SQL scripts to keep orchestration logic at the database level. The master proc owns sequencing, error handling, and run logging, so Python only needs a single call to execute the full layer.
- Writing to the run log from within the `TRY/CATCH` block is straightforward when the logic lives in SQL Server — doing the same from Python would mean catching SQL errors and issuing separate logging queries across two languages.
- Child procs are reusable database objects that can be called from anywhere, and adding a new table requires only a new child proc and two lines in the master proc with no changes to Python.

**CRM and ERP integration**
- The join strategy between CRM and ERP tables is being determined through data profiling. Each table is being analysed individually before cross-system relationships are defined.

---

## Data Sources

Raw CSV files are not version-controlled. The bronze table definitions act as the schema contract for the expected structure of source data. Source files are mounted into the Docker container and accessed via `BULK INSERT`.
