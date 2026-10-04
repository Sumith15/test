# Sales Performance & Customer Intelligence Platform

## 1. Title

**Sales Performance & Customer Intelligence Platform**

## 2. Problem Statement

A growing distributor needs a reliable way to understand how sales performance changes across products, customers, locations, and time. Operational data is fragmented and changes incrementally, making manual reports inconsistent and historical customer analysis unreliable. Build a trusted sales performance analytics platform that produces reconciled daily/monthly sales, product, and customer insights from the latest valid source state.

## 3. Project Objective

Create a trusted analytical view of sales performance that lets management analyze daily/monthly sales trends, top products/categories, top customers, historical city/segment performance, average order value, customer growth/decline, and product contribution.

The project is intentionally designed so that the **business problem requires the engineering capabilities in the capstone**, rather than adding technologies only for demonstration.

## 4. End-to-End Architecture

```text
Odoo PostgreSQL ───────┐
Customer API ──────────┤
Products CSV ──────────┤
Inventory Excel ───────┤
Supplier XML ──────────┤──> BRONZE (HDFS / Parquet)
Payment JSON ──────────┤              │
Kafka sales-events ────┘              ↓
                              SILVER (PySpark)
                           ┌───────────────┐
                           │ typing        │
                           │ validation    │
                           │ quarantine    │
                           │ deduplication  │
                           │ incremental    │
                           │ SCD1 / SCD2   │
                           │ late joins    │
                           └───────┬───────┘
                                   ↓
                              QUALITY GATE
                                   │
                         pass ─────┴───── fail
                          ↓                ↓
                       GOLD             Audit +
                          │             Quarantine
                          ↓
                    SNOWFLAKE
              DIMENSIONS + FACT + GOLD SQL
                          │
                          ↓
                  Business analytics /
                    early-warning views

Kafka also runs as a bounded Structured Streaming path:
Kafka → Spark Streaming → Bronze events → deduped event landing
```

**Core storage areas**

```text
hdfs://.../bronze/
hdfs://.../silver/
hdfs://.../gold/
hdfs://.../quarantine/
hdfs://.../audit/
```

## 5. Concepts From the Capstone Implemented


### Concepts demonstrated

| Concept | How it is used |
|---|---|
| Multi-source ingestion | PostgreSQL/Odoo, customer API, CSV, Excel, XML, JSON, Kafka |
| Bronze/Silver/Gold | Immutable evidence → trusted standardized data → business analytics |
| HDFS data lake | Persist Bronze, Silver, Gold, quarantine, and audit outputs |
| PySpark | Primary transformation, validation, joins, windows, aggregations |
| Explicit schemas | Every core dataset is read with a declared schema |
| Data quality | Required fields, timestamps, numeric types, ranges, references, duplicates |
| Quarantine | Preserve invalid records with payload/location, batch context, error code, description |
| Incremental processing | Process initial load and two change packs without full rebuild |
| Batch ledger / high-water mark | Track successful source boundaries and prevent skipped changes |
| Deterministic deduplication | Latest source version/modification order wins; exact replays are ignored |
| Equal-timestamp handling | Use source version/tie-break ordering so boundary records are never lost |
| SCD Type 2 | Customer city/segment with surrogate keys and `[effective_from, effective_to)` |
| SCD Type 1 | Product name/category corrections overwrite current attributes |
| Historical fact joins | Resolve customer version using order timestamp, including late orders |
| Decimal money arithmetic | `quantity × unit_price × (1 − discount/100)`, rounded to 2 decimals per line |
| Cancellation handling | Update existing order state; cancelled facts remain traceable but are KPI-ineligible |
| Reconciliation | Source → accepted/rejected/duplicate/superseded; financial bridge to Gold/Snowflake |
| Quality publication gate | Fail publication when invariants or configurable rejection threshold fail |
| Snowflake star schema | `DIM_CUSTOMER`, `DIM_PRODUCT`, `DIM_DATE`, `FACT_SALES` |
| Analytical SQL | Answer the capstone business questions from the warehouse |
| Structured Streaming | Consume `sales-events`, persist offsets/checkpoints and event payloads |
| Streaming deduplication | Persistent `event_id`-based deduplication across replay/restart |
| Late-event handling | Flag events where processing time is >10 minutes after event time |
| Airflow | Parameterized DAG with dependencies, retries, logs, rerun support |
| Recovery | Inject failure after intermediate write; rerun safely without duplicate business rows |
| Idempotency | Replaying a completed batch does not change logical output |
| Portability | Externalize paths/config so HDFS/local code can be adapted to S3/EMR |
| Auditability | Attempt IDs, counts, statuses, boundaries, timestamps and failed attempts retained |


### Project-specific concepts

This project gives strongest coverage to **Spark aggregations, dimensional modelling, SCD history, ranking/window functions, and reconciliation**. The streaming path remains a bounded visibility feature as required by the capstone.

## 6. Bronze Layer — Raw Data


### Bronze datasets

Bronze is an **immutable source-evidence layer**. Preserve the original payload/file/extracted record plus ingestion metadata. The schemas below are **logical source contracts** for planning; map them to the trainer-provided source column names rather than assuming these exact physical names.

Common metadata on every Bronze dataset:

| Column | Type | Purpose |
|---|---|---|
| `source_system` | STRING | `odoo`, `customer_api`, `product_csv`, `inventory_excel`, `supplier_xml`, `payment_json`, `kafka` |
| `source_reference` | STRING | File path, extraction ID, or Kafka topic/partition/offset |
| `batch_id` | STRING | Stable release/extraction identifier |
| `ingestion_timestamp` | TIMESTAMP | When the record entered Bronze |
| `raw_payload` | STRING / VARIANT-like text | Original payload where practical; mandatory for recoverability of malformed input |

#### 1. `bronze_sales_orders`

| Column | Type | Required | Notes |
|---|---|---|---|
| `order_id` | STRING | Yes | Stable order business key |
| `customer_id` | STRING | Yes | Customer business key |
| `order_ts` | TIMESTAMP | Yes | Business order timestamp |
| `order_status` | STRING | Yes | Draft / confirmed / cancelled |
| `source_modified_ts` | TIMESTAMP | Yes | Incremental ordering field |
| `source_version` | STRING / LONG | Yes | Deterministic source version |
| `currency` | STRING | Yes | Expected INR |

#### 2. `bronze_sales_order_lines`

| Column | Type | Required | Notes |
|---|---|---|---|
| `order_id` | STRING | Yes | Parent order key |
| `line_id` | STRING | Yes | Line key within order |
| `product_id` | STRING | Yes | Product business key |
| `quantity` | DECIMAL(18,3) | Yes | Must be > 0 |
| `unit_price` | DECIMAL(18,2) | Yes | Must be >= 0 |
| `discount_percentage` | DECIMAL(5,2) | Yes | 0 to 100 |
| `source_modified_ts` | TIMESTAMP | Yes | Incremental ordering field |
| `source_version` | STRING / LONG | Yes | Deterministic ordering |

#### 3. `bronze_customers`

| Column | Type | Required | Notes |
|---|---|---|---|
| `customer_id` | STRING | Yes | Stable source ID |
| `customer_name` | STRING | Yes | Descriptive attribute |
| `city` | STRING | Yes | SCD2 attribute |
| `segment` | STRING | Yes | SCD2 attribute |
| `business_effective_ts` | TIMESTAMP | Yes | When the customer change becomes effective |
| `source_modified_ts` | TIMESTAMP | Yes | Source change time |
| `source_version` | STRING / LONG | Yes | Deterministic version |

#### 4. `bronze_products`

| Column | Type | Required | Notes |
|---|---|---|---|
| `product_id` | STRING | Yes | Stable product ID |
| `product_name` | STRING | Yes | SCD1 attribute |
| `category` | STRING | Yes | SCD1 attribute |
| `source_modified_ts` | TIMESTAMP | Yes | Source change time |
| `source_version` | STRING / LONG | Yes | Deterministic version |

#### 5. `bronze_inventory_snapshots`

| Column | Type | Required | Notes |
|---|---|---|---|
| `snapshot_date` | DATE | Yes | Snapshot date |
| `product_id` | STRING | Yes | Product reference |
| `warehouse_id` | STRING | Yes | Warehouse/location |
| `stock_qty` | DECIMAL(18,3) | Yes | Quantity available in source snapshot |
| `file_name` | STRING | Yes | Original Excel file |
| `worksheet_name` | STRING | Useful | Source worksheet |
| `batch_id` | STRING | Yes | Ingestion batch |

This feed is an interface exercise in the core capstone; do not invent extra KPIs unless the chosen project explicitly uses the optional inventory extension.

#### 6. `bronze_supplier_product_refs`

| Column | Type | Required | Notes |
|---|---|---|---|
| `supplier_id` | STRING | Yes | Supplier reference |
| `supplier_product_id` | STRING | Yes | Supplier-side product key |
| `product_id` | STRING | Useful | Internal product key if supplied |
| `supplier_product_name` | STRING | Yes | Supplier description |
| `supplier_category` | STRING | Useful | Supplier category |
| `reference_valid_from` | TIMESTAMP | Useful | Source-effective timestamp |
| `raw_xml_reference` | STRING | Yes | Recoverable source location |

XML parsing failures must remain recoverable and be quarantined.

#### 7. `bronze_payment_events`

| Column | Type | Required | Notes |
|---|---|---|---|
| `payment_event_id` | STRING | Yes | Event identifier if supplied |
| `order_id` | STRING | Useful | Related order |
| `event_type` | STRING | Yes | Gateway event type |
| `event_ts` | TIMESTAMP | Yes | Event timestamp if parseable |
| `amount` | DECIMAL(18,2) | Useful | Payment amount if supplied |
| `currency` | STRING | Useful | Expected INR |
| `raw_payload` | STRING | Yes | Original JSON |

Payment events are retained for interface validation only in the core sales model; they must not be converted into recognized revenue.

#### 8. `bronze_sales_events`

| Column | Type | Required | Notes |
|---|---|---|---|
| `event_id` | STRING | Yes | Streaming dedup key |
| `order_id` | STRING | Useful | Related order/event context |
| `event_type` | STRING | Yes | Sales event type |
| `event_time` | TIMESTAMP | Yes | Business event time |
| `event_payload` | STRING | Yes | Original Kafka payload |
| `processing_timestamp` | TIMESTAMP | Yes | Consumer processing time |
| `topic` | STRING | Yes | Kafka topic |
| `partition` | INT | Yes | Kafka partition |
| `offset` | LONG | Yes | Kafka offset |
| `batch_id` | STRING | Yes | Fixture/replay batch identifier |


### Bronze partitioning and traceability

Recommended logical partitioning:

```text
bronze/<dataset>/batch_id=<batch_id>/
```

For date-heavy files or event data, an additional date partition can be used, but **`batch_id` and source reference must remain traceable**.

Bronze should not "fix" the source. Preserve:
- original values
- original payload/file reference
- batch ID
- source modification/effective timestamps
- ingestion timestamp
- source system/reference

## 7. Silver Layer — Transformations


### Step 1 — Source typing and standardization

Use **PySpark** to:
- apply explicit schemas
- normalize UTC timestamps
- standardize status values
- cast quantity/price/discount to decimal types
- retain source lineage columns

### Step 2 — Sales-order and line validation

Create trusted candidate datasets:
- validate `(order_id, line_id)` grain
- validate quantity > 0
- validate price >= 0
- validate discount between 0 and 100
- calculate `line_net_value`
- quarantine invalid rows with actionable error codes

### Step 3 — Deterministic incremental upsert

Use `(business_key, source_version/source_modified_ts)` ordering to:
- keep the latest valid source state
- detect exact replays
- classify duplicate deliveries
- classify superseded versions
- surface conflicting same-key/same-version records

Track progress with an **Airflow-controlled batch ledger/high-water mark**.

### Step 4 — Customer SCD Type 2

Using **PySpark window functions**:
- order customer changes by business-effective timestamp + source ordering
- create surrogate keys
- build non-overlapping `[effective_from, effective_to)` intervals
- set `is_current`
- do not create a new version for unchanged replays

### Step 5 — Product SCD Type 1

For each `product_id`:
- determine latest accepted product version
- overwrite name/category with the latest corrected values
- keep one logical current row

### Step 6 — Historical customer resolution

For each sales line:
```text
customer_id
AND order_ts >= effective_from
AND order_ts < effective_to
```

Resolve the correct historical `customer_sk`. This is essential for reporting city/segment **at order time**.

### Step 7 — Fact preparation

Join:
```text
silver_order_lines
      + latest trusted order state
      + DIM/temporary customer SCD2
      + latest product SCD1
      + DIM_DATE mapping
```

Produce one latest accepted business state per `(order_id, line_id)`.

### Step 8 — Gold-ready aggregates

Use **PySpark groupBy + window functions** to derive:
- daily sales
- monthly sales
- customer trends
- product contribution
- customer/product ranking attributes

### Step 9 — Reconciliation

Compare:
```text
latest valid eligible source
        vs
Silver
        vs
Gold
        vs
Snowflake
```

Use decimal exactness; financial difference must be zero after documented adjustments.


### Shared Silver rules

1. Apply explicit schemas before business transformation.
2. Normalize timestamps to UTC.
3. Validate required keys and business ranges.
4. Calculate line net value with decimal arithmetic and round each line to two decimals before aggregation.
5. Classify multi-error records once using a documented priority order.
6. Deduplicate using business key + source ordering, never ingestion time alone.
7. Surface conflicting records with the same key and source version.
8. Quarantine unresolved customer/product references.
9. Maintain a stable batch ledger/high-water mark.
10. Publish only after the quality gate passes.
11. Never silently discard malformed files or records.

## 8. Snowflake Core Star Schema

The required warehouse remains:

### `DIM_CUSTOMER` — SCD Type 2

| Column | Type | Purpose |
|---|---|---|
| `customer_sk` | NUMBER | Surrogate key |
| `customer_id` | VARCHAR | Business key |
| `customer_name` | VARCHAR | Customer name |
| `city` | VARCHAR | Historical city |
| `segment` | VARCHAR | Historical segment |
| `effective_from` | TIMESTAMP | Version start |
| `effective_to` | TIMESTAMP | Exclusive version end |
| `is_current` | BOOLEAN | Current version |

### `DIM_PRODUCT` — SCD Type 1

| Column | Type | Purpose |
|---|---|---|
| `product_sk` | NUMBER | Surrogate key |
| `product_id` | VARCHAR | Business key |
| `product_name` | VARCHAR | Latest product description |
| `category` | VARCHAR | Latest category |
| `updated_at` | TIMESTAMP | Last accepted product update |

### `DIM_DATE`

| Column | Type | Purpose |
|---|---|---|
| `date_sk` | NUMBER | Date surrogate key |
| `calendar_date` | DATE | Reporting date |
| `day_of_week` | NUMBER | Calendar attribute |
| `month` | NUMBER | Month number |
| `month_name` | VARCHAR | Month label |
| `quarter` | NUMBER | Quarter |
| `year` | NUMBER | Year |

### `FACT_SALES` — latest accepted business state at order-line grain

| Column | Type | Purpose |
|---|---|---|
| `sales_sk` | NUMBER | Fact surrogate key |
| `order_id` | VARCHAR | Order business key |
| `line_id` | VARCHAR | Line business key |
| `customer_sk` | NUMBER | Historical customer FK |
| `product_sk` | NUMBER | Product FK |
| `date_sk` | NUMBER | Date FK |
| `order_ts` | TIMESTAMP | Order time |
| `order_date` | DATE | UTC reporting date |
| `status` | VARCHAR | Current order status |
| `quantity` | DECIMAL(18,3) | Quantity |
| `unit_price` | DECIMAL(18,2) | Unit price |
| `discount_percentage` | DECIMAL(5,2) | Discount |
| `line_net_value` | DECIMAL(18,2) | Rounded line net value |
| `source_version` | VARCHAR | Source ordering evidence |

Cancelled lines stay in the fact with current status, but analytical filters must use **confirmed/eligible** rows.

## 9. Gold Layer


### Required / recommended Gold tables

#### `GOLD_DAILY_SALES`
**Grain:** one row per UTC order date.

| Column | Type |
|---|---|
| `order_date` | DATE |
| `confirmed_orders` | BIGINT |
| `cancelled_orders` | BIGINT |
| `eligible_orders` | BIGINT |
| `unique_customers` | BIGINT |
| `units_sold` | DECIMAL(18,3) |
| `net_sales` | DECIMAL(18,2) |
| `avg_order_value` | DECIMAL(18,2) |

#### `GOLD_MONTHLY_SALES`
**Grain:** one row per calendar month.

| Column | Type |
|---|---|
| `year` | INT |
| `month` | INT |
| `confirmed_orders` | BIGINT |
| `eligible_orders` | BIGINT |
| `units_sold` | DECIMAL(18,3) |
| `net_sales` | DECIMAL(18,2) |
| `avg_order_value` | DECIMAL(18,2) |

#### `GOLD_PRODUCT_PERFORMANCE`
**Grain:** one row per `order_date + product_id`.

| Column | Type |
|---|---|
| `order_date` | DATE |
| `product_id` | VARCHAR |
| `product_name` | VARCHAR |
| `category` | VARCHAR |
| `orders` | BIGINT |
| `units_sold` | DECIMAL(18,3) |
| `net_sales` | DECIMAL(18,2) |
| `avg_unit_net_value` | DECIMAL(18,2) |
| `sales_contribution_pct` | DECIMAL(8,4) |

#### `GOLD_CUSTOMER_SALES`
**Grain:** one row per `order_date + customer_id + historical customer version`.

| Column | Type |
|---|---|
| `order_date` | DATE |
| `customer_id` | VARCHAR |
| `customer_sk` | NUMBER |
| `customer_name` | VARCHAR |
| `city_at_order` | VARCHAR |
| `segment_at_order` | VARCHAR |
| `orders` | BIGINT |
| `units_sold` | DECIMAL(18,3) |
| `net_sales` | DECIMAL(18,2) |
| `avg_order_value` | DECIMAL(18,2) |

#### `GOLD_CUSTOMER_TRENDS`
**Grain:** one row per `month + customer_id`.

| Column | Type |
|---|---|
| `year_month` | DATE / VARCHAR |
| `customer_id` | VARCHAR |
| `net_sales` | DECIMAL(18,2) |
| `previous_period_sales` | DECIMAL(18,2) |
| `sales_change_pct` | DECIMAL(9,4) |
| `active_orders` | BIGINT |

#### `GOLD_PRODUCT_CONTRIBUTION`
**Grain:** one row per `month + product/category`.

| Column | Type |
|---|---|
| `year_month` | DATE / VARCHAR |
| `product_id` | VARCHAR |
| `category` | VARCHAR |
| `net_sales` | DECIMAL(18,2) |
| `category_sales` | DECIMAL(18,2) |
| `contribution_pct` | DECIMAL(8,4) |
| `category_rank` | INT |


### Gold-to-Silver / Snowflake mapping


### Mapping

```text
FACT_SALES
  ← silver latest order state
  ← silver validated order lines
  ← customer SCD2 lookup by order timestamp
  ← product SCD1 lookup by product_id
  ← DIM_DATE by order_date

GOLD_DAILY_SALES
  ← FACT_SALES
  WHERE status = 'confirmed'

GOLD_MONTHLY_SALES
  ← GOLD_DAILY_SALES / FACT_SALES
  GROUP BY year, month

GOLD_PRODUCT_PERFORMANCE
  ← FACT_SALES
  JOIN DIM_PRODUCT
  GROUP BY order_date, product_id, product_name, category

GOLD_CUSTOMER_SALES
  ← FACT_SALES
  JOIN DIM_CUSTOMER (already resolved to order-time customer_sk)
  GROUP BY order_date, customer_id, city_at_order, segment_at_order

GOLD_CUSTOMER_TRENDS
  ← GOLD_CUSTOMER_SALES
  WINDOW: LAG(net_sales) OVER (PARTITION BY customer_id ORDER BY month)

GOLD_PRODUCT_CONTRIBUTION
  ← GOLD_PRODUCT_PERFORMANCE
  WINDOW: SUM(net_sales) OVER (PARTITION BY month/category)
```


## 10. Business Questions / Analytical SQL

1. What is the net value of confirmed sales by day and month?
2. Which products/categories generate the highest net sales value?
3. Who are the top customers, and how do sales vary by historical customer segment and city?
4. What is the average order value for each day?
5. How many source records were rejected, superseded, or identified as duplicates, and why?

Additional project analytics:
- Which customers increased/decreased sales compared with the previous month?
- Which products contribute most to monthly/category sales?


All SQL should use the same source cutoff and filters as the Gold transformations. Gold and Snowflake monetary totals must reconcile exactly after applying the declared rounding rules.

## 11. Data Quality and Publication Gate

Before Gold or warehouse business tables are published:

```text
required keys               = no nulls
declared Silver/fact grain  = no duplicate logical keys
customer/product refs       = all resolvable
quantity                    > 0
unit_price                  >= 0
discount                    BETWEEN 0 AND 100
calculated amounts          = valid decimal values
core-source reject rate     <= 5% (configurable)
```

If the gate fails:
- preserve Bronze
- preserve quarantine
- preserve audit attempt
- mark batch failed
- do not publish Gold/warehouse business changes

## 12. Audit and Reconciliation

For every batch:

```text
records_read
records_accepted
records_rejected
records_duplicate
records_superseded
```

Required accounting:

```text
records_read
= accepted_records
+ rejected_records
+ duplicate_records
+ superseded_records
```

Financial reconciliation:

```text
Latest valid eligible source state
        ↓
Silver trusted sales
        ↓
Gold aggregate
        ↓
Snowflake FACT_SALES / analytical SQL
```

Provide a bridge for:
- invalid records
- duplicate deliveries
- superseded versions
- cancellations
- non-eligible statuses
- source cutoff differences

## 13. Streaming Blueprint

Use the trainer-owned `sales-events` Kafka topic.

```text
Kafka sales-events
      ↓
Spark Structured Streaming
      ↓
Explicit schema
      ↓
Malformed-message quarantine
      ↓
Bronze payload + offset persistence
      ↓
event_id deduplication
      ↓
late-event flag
      ↓
Silver/landing event dataset
```

Rules:
- persistent checkpoint outside temporary process directories
- one accepted logical event per distinct valid `event_id`
- replay after restart must not create a duplicate logical event
- `processing_timestamp - event_time > 10 minutes` → `is_late = true`
- valid events should land within 60 seconds during the demonstration
- event stream is for visibility and does **not** feed `FACT_SALES` in the core design

## 14. Airflow DAG

Recommended DAG:

```text
start
  ↓
extract_core_sources
  ↓
bronze_validation
  ↓
silver_orders ─────────────┐
silver_order_lines         │
silver_customers_scd2 ─────┼──> quality_gate
silver_products_scd1 ──────┘        ↓
                            customer/product prep
                                     ↓
                               gold_publication
                                     ↓
                              snowflake_dimensions
                                     ↓
                              snowflake_fact_sales
                                     ↓
                            reconciliation_check
                                     ↓
                                    end
```

Add:
- configurable `batch_id`
- retries
- clear failure logs
- no overlapping publication attempts for the same batch
- rerun by batch
- controlled failure after an intermediate write
- recovery verification against a clean run

## 15. Failure-Recovery Demonstration

Test case:

1. Run Pack 02 and inject a controlled failure after a Silver write.
2. Leave Bronze, Silver partial output, quarantine and audit evidence intact.
3. Resume Pack 02 using the same `batch_id`.
4. Ensure deterministic upsert/replacement logic removes duplicate logical output.
5. Compare:
   - Silver key counts
   - customer SCD2 versions
   - product current state
   - fact rows
   - Gold metrics
   - financial totals
6. Confirm they match a clean execution.

## 16. Portability

All environment-specific values must be externalized:

```text
HDFS base path
Snowflake account / database / schema / warehouse
source connection
API endpoint/credentials
Kafka bootstrap servers
checkpoint locations
batch configuration
```

The same transformation modules should be capable of a future S3/EMR profile through configuration changes rather than hard-coded HDFS paths.

## 17. Suggested Repository Structure

```text
project/
├── airflow/
│   └── dags/
├── config/
│   ├── local.yaml
│   └── aws_emr.yaml
├── ingestion/
│   ├── odoo_sales.py
│   ├── customer_api.py
│   ├── product_csv.py
│   ├── inventory_excel.py
│   ├── supplier_xml.py
│   ├── payment_json.py
│   └── kafka_stream.py
├── spark/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── quality/
│   └── reconciliation/
├── snowflake/
│   ├── ddl/
│   └── sql/
├── tests/
├── evidence/
└── README.md
```

## 18. Tool Stack


### Tooling by process

| Process | Tool | Responsibility |
|---|---|---|
| Source extraction control | **Airflow + Python** | Parameters, batch IDs, retries, task dependencies |
| Odoo sales extraction | **PostgreSQL + Python/psycopg2 (or provided connector)** | Read-only extraction from trainer views |
| Customer API extraction | **Python Requests + provided adapter** | Pagination, updates, extraction boundary |
| Product CSV ingestion | **PySpark** | Typed read and Bronze persistence |
| Excel ingestion | **Python/openpyxl or provided parser + PySpark** | Worksheet parsing, schema validation, Bronze output |
| XML ingestion | **Provided parser / Python XML tooling + PySpark** | Parse and preserve source evidence |
| Payment JSON ingestion | **PySpark JSON reader** | Explicit schema and malformed-record isolation |
| Kafka consumption | **Spark Structured Streaming** | Bounded event ingestion, offsets, checkpointing |
| Lake storage | **HDFS + Parquet** | Durable Bronze/Silver/Gold storage |
| Transformations | **PySpark** | Casts, validation, joins, windows, deduplication, aggregates |
| Data quality gate | **PySpark + audit/control tables** | Enforce row-level and batch-level invariants |
| Orchestration | **Apache Airflow** | End-to-end batch DAG and recovery |
| Warehouse | **Snowflake** | Dimensions, fact, Gold publication and analytical SQL |
| Warehouse loading | **Snowflake Connector / provided local loading path** | Load validated datasets into Snowflake |
| Version control | **Git** | Reproducible code/config/documentation |
| Optional cloud portability | **AWS S3 + EMR** | Same transformations with configuration-only path changes |


## 19. Acceptance-Criteria Coverage

| Capstone acceptance area | This project demonstrates |
|---|---|
| AC01 ingestion | All declared source interfaces |
| AC02 layer separation | Bronze/Silver/Gold + quarantine |
| AC03 quality gate | Rule validation + reject threshold |
| AC04 incremental changes | Pack 01 → Pack 02 → Pack 03 |
| AC05 idempotency | Replay without logical changes |
| AC06 history | Customer SCD2 + product SCD1 |
| AC07 warehouse correctness | Star schema + fact grain |
| AC08 business answers | Project-specific Gold + Snowflake SQL |
| AC09 reconciliation | Record and financial balancing |
| AC10 streaming | Kafka + Structured Streaming + restart/replay |
| AC11 orchestration/recovery | Airflow + controlled failure |
| AC12 reproducibility | Config-driven local execution |

## 20. Expected Final Deliverables

- PySpark ingestion/transformation modules
- HDFS Bronze/Silver/Gold datasets
- Quarantine outputs
- Audit/control tables
- Airflow DAG
- Kafka Structured Streaming consumer
- Snowflake DDL
- Snowflake analytical SQL
- Source-to-target mapping
- Data-quality rules
- Reconciliation report
- Controlled-failure recovery evidence
- Streaming replay/restart evidence
- Concise execution README

## 21. Scope Guardrails

The core capstone does **not** require:
- invoice/payment allocation modelling
- production CDC administration
- a real-time warehouse
- a dashboard application
- hard deletes
- retroactive SCD repair

Optional work should only be attempted after the core acceptance criteria pass.
