# Customer 360 & Historical Sales Intelligence

## 1. Title

**Customer 360 & Historical Sales Intelligence**

## 2. Problem Statement

Customer city and segment change over time, but conventional reporting often attributes historical orders to the customer's latest attributes. This can distort historical customer analysis. Build a Customer 360 and Historical Sales Intelligence platform that reconstructs the customer context that existed when each order was placed and uses that history to measure customer value, movement, and sales performance.

## 3. Project Objective

Build a historically accurate customer-centric analytical model covering customer sales by historical segment and city, customer lifetime sales, segment/city performance, customer movement between segments, and before/after sales around customer-attribute changes.

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

The project-specific showcase is **temporal modelling**: SCD2, point-in-time joins, customer version analytics, change detection with window functions, and before/after analysis. Streaming and the file-interface feeds remain in scope because the capstone requires them, but they do not need to drive Customer 360 KPIs.

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


### Step 1 — Customer change stream to typed records

Use **PySpark** to:
- apply the customer API schema
- normalize business-effective timestamps to UTC
- validate customer IDs and required attributes
- order versions deterministically

### Step 2 — Customer SCD Type 2 construction

For every `customer_id`:
- sort by `business_effective_ts`, then source version/modification order
- compare city/segment to the previous accepted version
- generate a surrogate key only for a genuine historical change
- derive `effective_from`
- derive `effective_to` with the next version's start
- set exactly one `is_current = true`
- use `[effective_from, effective_to)` intervals

Use **PySpark window functions: `LAG`, `LEAD`, `ROW_NUMBER`**.

### Step 3 — Sales validation and incremental state

Process sales orders/lines exactly as in the capstone:
- validate required keys and ranges
- calculate decimal line net value
- deterministic deduplication
- latest accepted business state
- quarantine invalid customer/product references

### Step 4 — Historical customer-to-order resolution

Join sales to the SCD2 customer dataset on:
```text
sales.customer_id = customer.customer_id
AND sales.order_ts >= customer.effective_from
AND sales.order_ts < customer.effective_to
```

This produces the customer surrogate key representing the customer's historical state when the order happened.

### Step 5 — Customer lifecycle metrics

Derive Silver features such as:
- first eligible order timestamp
- latest eligible order timestamp
- order count
- cumulative net sales
- active months
- prior segment
- current segment
- segment-change effective timestamp

### Step 6 — Product SCD1 and fact preparation

Resolve product IDs to the latest product description/category. Build the latest accepted sales-line state and retain cancelled facts for traceability.

### Step 7 — Before/after change windows

For each customer change event:
- identify orders before `effective_from`
- identify orders after `effective_from`
- aggregate sales over a declared comparison window

Keep the comparison window configurable, e.g. previous 30 days vs following 30 days, rather than hard-coding business semantics.

### Step 8 — Reconciliation

Reconcile customer-centric Gold outputs back to the same trusted sales state used by `FACT_SALES`. Customer history is a dimensioning mechanism; it must not alter the financial truth of the sales fact.


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


### `GOLD_CUSTOMER_360`
**Grain:** one row per customer.

| Column | Type |
|---|---|
| `customer_id` | VARCHAR |
| `customer_name` | VARCHAR |
| `current_city` | VARCHAR |
| `current_segment` | VARCHAR |
| `first_order_date` | DATE |
| `last_order_date` | DATE |
| `lifetime_orders` | BIGINT |
| `lifetime_units` | DECIMAL(18,3) |
| `lifetime_net_sales` | DECIMAL(18,2) |
| `lifetime_aov` | DECIMAL(18,2) |
| `segment_change_count` | BIGINT |
| `active_months` | BIGINT |

### `GOLD_CUSTOMER_SEGMENT_HISTORY`
**Grain:** one row per customer SCD2 version.

| Column | Type |
|---|---|
| `customer_id` | VARCHAR |
| `customer_sk` | NUMBER |
| `city` | VARCHAR |
| `segment` | VARCHAR |
| `effective_from` | TIMESTAMP |
| `effective_to` | TIMESTAMP |
| `orders_in_version_period` | BIGINT |
| `sales_in_version_period` | DECIMAL(18,2) |
| `aov_in_version_period` | DECIMAL(18,2) |

### `GOLD_CUSTOMER_MOVEMENT`
**Grain:** one row per customer segment/city change.

| Column | Type |
|---|---|
| `customer_id` | VARCHAR |
| `change_ts` | TIMESTAMP |
| `old_city` | VARCHAR |
| `new_city` | VARCHAR |
| `old_segment` | VARCHAR |
| `new_segment` | VARCHAR |
| `sales_before_window` | DECIMAL(18,2) |
| `sales_after_window` | DECIMAL(18,2) |
| `orders_before_window` | BIGINT |
| `orders_after_window` | BIGINT |

### `GOLD_SEGMENT_CITY_SALES`
**Grain:** one row per `order_date + historical segment + historical city`.

| Column | Type |
|---|---|
| `order_date` | DATE |
| `segment_at_order` | VARCHAR |
| `city_at_order` | VARCHAR |
| `customers` | BIGINT |
| `orders` | BIGINT |
| `units_sold` | DECIMAL(18,3) |
| `net_sales` | DECIMAL(18,2) |
| `avg_order_value` | DECIMAL(18,2) |

### `GOLD_CUSTOMER_MONTHLY_TREND`
**Grain:** one row per `month + customer`.

| Column | Type |
|---|---|
| `year_month` | DATE / VARCHAR |
| `customer_id` | VARCHAR |
| `net_sales` | DECIMAL(18,2) |
| `orders` | BIGINT |
| `previous_month_sales` | DECIMAL(18,2) |
| `sales_change_pct` | DECIMAL(9,4) |
| `cumulative_sales` | DECIMAL(18,2) |


### Gold-to-Silver / Snowflake mapping


### Mapping

```text
DIM_CUSTOMER
  ← bronze_customers
  → deterministic version ordering
  → SCD2 effective interval generation

FACT_SALES
  ← validated orders + lines
  ← historical customer_sk from order timestamp
  ← latest product_sk
  ← DIM_DATE

GOLD_CUSTOMER_360
  ← FACT_SALES
  JOIN DIM_CUSTOMER
  GROUP BY customer_id

GOLD_CUSTOMER_SEGMENT_HISTORY
  ← DIM_CUSTOMER
  LEFT JOIN FACT_SALES
    ON customer_sk
  GROUP BY customer version

GOLD_CUSTOMER_MOVEMENT
  ← DIM_CUSTOMER
  WINDOW LAG(city), LAG(segment)
  JOIN FACT_SALES for configurable before/after windows

GOLD_SEGMENT_CITY_SALES
  ← FACT_SALES
  JOIN DIM_CUSTOMER
  GROUP BY order_date, historical city, historical segment

GOLD_CUSTOMER_MONTHLY_TREND
  ← FACT_SALES
  JOIN DIM_CUSTOMER
  GROUP BY month + customer
  WINDOW LAG + cumulative SUM
```


## 10. Business Questions / Analytical SQL

1. What is the lifetime sales value of each customer?
2. How much did each customer contribute while in each historical segment/city?
3. Which customer segments/cities generated the most net sales over time?
4. Which customers changed segment, and what were their before/after sales and order levels?
5. How many source records were rejected, superseded, or identified as duplicates, and why?

Key analytical demonstration:
- Show a customer whose current segment differs from the segment stored against an older order, proving why an SCD2 point-in-time join is necessary.


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
