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
└── gold/
    ├── create_tables.sql                # Gold table DDL (dim_customer, dim_product, fact_sales)
    ├── dim_customer.sql                 # dim_customer transformation and load proc
    ├── dim_product.sql                  # dim_product transformation and load proc
    ├── fact_sales.sql                   # fact_sales transformation and load proc
    └── load_orchestration.sql           # Master proc: deletes fact_sales, loads dims, loads fact

python/
├── db_utils.py                         # Database connection helpers (get_connection, get_cursor)
├── config.yaml                         # Layer metadata (primary keys, referential integrity refs) for Silver and Gold
├── paths.py                            # Centralised file path constants
├── logging_config.py                   # Root logger setup for pipeline-level error logging
├── bronze_pipeline_orchestrator.py     # Bronze layer orchestration
├── silver_pipeline_orchestrator.py     # Silver layer orchestration
├── gold_pipeline_orchestrator.py       # Gold layer orchestration
├── bronze_data_validation.py           # Bronze row-count validation
├── silver_data_validation.py           # Silver validation (nulls, duplicates, referential integrity)
├── gold_data_validation.py             # Gold validation (nulls, duplicates, referential integrity, flag percentages)
├── master_orchestrator.py              # End-to-end pipeline runner — executes all three layers in sequence
└── data_profiling.py                   # Exploratory data profiling
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

| Column                   | Purpose                                          |
|--------------------------|--------------------------------------------------|
| `id`                     | Auto-incrementing row identifier                 |
| `run_id`                 | UUID grouping all steps in a single run          |
| `layer`                  | Pipeline layer (`bronze`, `silver`, etc.)        |
| `proc_name`              | Stored procedure that executed                   |
| `run_start_timestamp`    | When the proc started                            |
| `run_end_timestamp`      | When the proc finished                           |
| `proc_run_time_ms`       | Execution duration in milliseconds               |
| `status`                 | `STARTED` → `SUCCESS` or `FAILED`               |
| `validation_status`      | `PASS` or `FAIL` (set by Python validation)      |
| `rows_read`              | Row count from source (set by Python)            |
| `rows_written`           | Row count loaded to table (set by Python)        |
| `referential_integrity`  | `PASS` or `FAIL` for referential integrity check |
| `null_key_check`         | `PASS` or `FAIL` for null key check              |
| `duplicate_key_check`    | `PASS` or `FAIL` for duplicate key check         |
| `error_class`            | Categorised failure type                         |
| `error_message`          | SQL error detail on failure                      |

**Python layer:**
- `bronze_pipeline_orchestrator.py` — executes the SQL master proc, reads post-run log output, and raises a custom `BronzePipelineFailed` exception on failure
- `data_validation.py` — compares row counts between source CSVs and bronze tables; on mismatch, marks affected tables as `FAILED` in the run log and raises `RowMismatch` or `KeyMismatch`

---

## Silver Layer (Complete)

The silver layer cleans and standardises raw bronze data. Transformations are applied in staged CTEs, each with a single clearly-named responsibility. Silver does not impute missing values or apply business-rule-based enrichment — those decisions are deferred to Gold.

**All six tables complete:**

| Table                        | Key transformations                                                                         |
|------------------------------|---------------------------------------------------------------------------------------------|
| `crm_cust_info`              | Type casting, whitespace trimming, duplicate resolution (fewest nulls → latest date)        |
| `crm_prd_info`               | Key splitting, product line expansion, end date reconstruction from next start date         |
| `crm_sales_details`          | Date format conversion, financial field derivation and reconciliation, bad data flagging    |
| `erp_cust_az12`              | Customer ID normalisation to cst_key format, date casting, gender standardisation           |
| `erp_loc_a101`               | Country code standardisation, whitespace trimming                                           |
| `erp_px_cat_g1v2`            | Whitespace trimming, maintenance flag standardisation                                       |

**Python layer:**
- `silver_pipeline_orchestrator.py` — executes the SQL master proc and raises a custom exception on failure
- `silver_data_validation.py` — runs three checks against all six silver tables: null checks on primary key columns, duplicate checks (including composite keys), and referential integrity checks across table relationships; results are written to `admin.etl_run_log` per table; failures bubble up as `SilverValidationFailed`

**Silver metadata (`config.yaml`):**
- Primary key columns per table (used for null and duplicate checks)
- Referential integrity relationships between tables (used for join-based orphan checks)

---

## Gold Layer (Complete)

The gold layer exposes a star schema dimensional model optimised for analytical queries. It interprets and enriches Silver data — applying business rules, imputing values, and resolving data quality issues deferred from Silver.

**SQL layer (complete):**
- Three child stored procs (`dim_customer`, `dim_product`, `fact_sales`) handle transformation and load
- `load_orchestration.sql` — master proc that deletes `fact_sales`, truncates and reloads both dims, then loads `fact_sales`; orchestrates all three child procs with structured `TRY/CATCH` and run logging

**Python layer (complete):**
- `gold_pipeline_orchestrator.py` — fetches the Silver `run_id`, executes the Gold master proc, and raises `GoldPipelineFailed` if any step fails
- `gold_data_validation.py` — runs three standard checks (null, duplicate, referential integrity) against all Gold tables, logs flag percentages for `fact_sales` data quality flags, updates `validation_status` in `admin.etl_run_log`, and raises `GoldValidationFailed` on any check failure
- `master_orchestrator.py` — end-to-end pipeline runner; executes all three layers in sequence (source → bronze → silver → gold, each with pipeline then validation); stops on any failure, logs to `pipeline.log` via root logger, prints detail to console, and exits with `sys.exit(1)`

**Three tables:**

| Table | Grain | Source tables |
|---|---|---|
| `gold.dim_customer` | One row per customer | `crm_cust_info`, `erp_cust_az12`, `erp_loc_a101` |
| `gold.dim_product` | One row per product version (SCD Type 2) | `crm_prd_info`, `erp_px_cat_g1v2` |
| `gold.fact_sales` | One row per order line | `crm_sales_details` + `gold.dim_product` |

**Data quality flags in `fact_sales`:**

| Flag | Meaning |
|---|---|
| `is_incomplete_financial_data` | Both `sales` and `price` are NULL — missing value cannot be derived |
| `err_date_lifecycle` | `order_date` falls outside all product version date ranges — `product_id` is NULL |
| `err_date_sequence` | `order_date <= ship_date <= delivery_date` is violated |

**Deferred items from Silver — resolved in Gold:**

| Item | Resolution |
|------|------------|
| NULL order date | Borrowed from a sibling row with the same order number via window function; ship/due dates not borrowed as these can legitimately differ across order lines |
| Negative sales | Converted to absolute value — return detection not possible without a reliable source-system return identifier |
| Incomplete financial data | Preserved with `is_incomplete_financial_data = 'Y'` flag; rows kept for auditability |
| Date chronological validation | Violations flagged with `err_date_sequence = 'Y'`; dates and financial data preserved |
| Unresolvable product version | Flagged with `err_date_lifecycle = 'Y'`; `product_id` left as NULL with `product_key` retained for manual lookup |

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
- Silver loads use truncate-and-reload. The production standard for handling source corrections is upsert (MERGE) or alternatives such as watermark loading or Change Data Capture (CDC). These were not chosen for the following reasons: the source is a static, one-time dataset with no incremental batches (ruling out watermark loading); CDC requires infrastructure complexity beyond the scope of this project; and upsert requires a reliable unique key per table, which is not naturally available across all six Silver tables. Generating a surrogate key via NEWID() would not survive a Bronze truncate-and-reload as it is non-deterministic. Truncate-and-reload is therefore the simplest correct approach for this dataset, and is idempotent by design.

**CRM and ERP integration**
- The join strategy between CRM and ERP tables is being determined through data profiling. Each table is being analysed individually before cross-system relationships are defined.

**Gold — star schema over snowflake**
- Gold uses a star schema: `fact_sales` is surrounded by `dim_customer` and `dim_product`, with no joins between dimension tables. A snowflake schema was considered but rejected. Neither `dim_customer` (which consolidates three Silver tables) nor `dim_product` (which consolidates two) has a hierarchy complex enough to warrant managing as separate normalised entities. The dataset is also small, so the storage redundancy from denormalisation is negligible. The star schema's benefit — a single join from fact to any dimension — outweighs the marginal gains of normalising further.

**Gold — data quality flags over row filtering**
- Rows with data quality issues in `fact_sales` (unresolvable product versions, date sequence violations, incomplete financial data) are preserved with flag columns rather than filtered out. Filtering would silently remove sales transactions from all downstream analysis, understating revenue and hiding the extent of the data quality problem. Flags allow analysts to make an informed choice about inclusion, and make the quality issue visible and measurable.

**Silver — known data quality fix**
- A referential integrity mismatch was identified between `crm_prd_info.cat_id` and `erp_px_cat_g1v2.id`: the category code for bike pedal products was recorded as `CO_PE` in the CRM source but `CO_PD` in the ERP category catalogue. Since `erp_px_cat_g1v2` is the authoritative source for category definitions, the correction was applied in the `crm_prd_info` Silver casting CTE — `CO_PE` is remapped to `CO_PD` with an explanatory comment. This fix is embedded in the transformation proc so it survives pipeline re-runs.

**Gold — DELETE before TRUNCATE for dimensions**
- The Gold master proc issues `DELETE FROM gold.fact_sales` before calling the dim child procs. SQL Server blocks `TRUNCATE` on any table that has a foreign key constraint pointing to it, regardless of whether the referencing table is empty — this is a metadata-level restriction, not a data-level one. `DELETE` has no such restriction. The dim child procs therefore use `DELETE` rather than `TRUNCATE` to clear their tables before reloading.

**Gold — `rows_read` convention**
- Each Gold child proc reads from multiple Silver tables via joins. `rows_read` in `admin.etl_run_log` is set to the row count of the driving (left-most) Silver table for each proc: `crm_cust_info` for `dim_customer`, `crm_prd_info` for `dim_product`, and `crm_sales_details` for `fact_sales`. Since all joins are LEFT JOINs, the driving table's row count equals the output row count before filtering — making it a meaningful and consistent metric.

**Gold validation — logging over run log columns**
- Silver validation writes per-check results (`PASS`/`FAIL`) to dedicated columns in `admin.etl_run_log`. Gold validation instead logs check detail to a dedicated `gold_validation.log` file using Python's `logging` module, and only writes a summary `validation_status` (`SUCCESS`/`FAIL`) to the run log. This separates operational status (run log) from diagnostic detail (log file), avoids adding Gold-specific columns to a shared table, and keeps the run log readable across layers. Pipeline-level errors also propagate to `pipeline.log` via the root logger.

**Progressive development — intentional non-uniformity**
- This project was built incrementally as a learning exercise. Patterns, structure, and code quality evolved across layers — earlier layers reflect earlier understanding, and later layers reflect more refined thinking. Conventions such as the Python `logging` module were introduced partway through and applied from that point forward rather than retrofitted. No refactoring was applied retrospectively, as the goal was forward progress and embedded learning rather than a production-grade codebase. Where known improvements exist, they are documented as assumptions or inline notes rather than implemented.

**Silver validation — dynamic queries over f-string SQL**
- In production, validation would typically use one script per table (as in dbt), allowing fully static, parameterised SQL. Because this project consolidates all six tables into a single validation script, table and column names must be interpolated dynamically using f-strings and Python loops. This carries a theoretical SQL injection risk. In this project, all dynamic values come from a controlled `config.yaml` file with no user input, so the risk is accepted. In a production system with external input, parameterised queries or an ORM would be required.

---

## Data Sources

Raw CSV files are not version-controlled. The bronze table definitions act as the schema contract for the expected structure of source data. Source files are mounted into the Docker container and accessed via `BULK INSERT`.
