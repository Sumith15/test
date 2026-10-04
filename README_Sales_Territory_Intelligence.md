# Sales Territory Intelligence

## 1. Title

**Sales Territory Intelligence Platform**

---

## 2. Problem Statement

A growing distributor needs to understand how sales performance differs across geographic territories, customer segments, products, and time. However, customer locations and sales records change over time, while operational data arrives from multiple independent sources with duplicates, invalid records, cancellations, late updates, and incremental changes.

Build a **trusted Sales Territory Intelligence Platform** that transforms these heterogeneous operational feeds into reconciled territory-level analytics, allowing management to identify high-performing and declining territories, understand the products and customer segments driving each territory, and compare territorial growth over time using the customer's historical location at the time of each order.

---

# 3. Business Objective

The platform should answer:

> **Where is the business growing, where is it declining, what is driving the change, and can management trust the numbers?**

The final analytics should provide:

- Territory-level daily and monthly sales
- Territory growth and decline
- Top and underperforming territories
- Product and category performance inside each territory
- Customer segment performance inside each territory
- Top customers within each territory
- Customer concentration by territory
- Average order value by territory
- Order and cancellation behavior by territory
- Changes in territory performance between incremental source releases
- Data-quality indicators showing whether an apparent business change may actually be caused by poor source data

The important design principle is that **the business question requires the engineering**, rather than adding technologies only because they are present in the capstone.

---

# 4. What Is a Territory?

The trainer-provided core customer source contains `customer_id`, `city`, and `segment`. It does **not** require a separate upstream territory master.

Therefore, territory is a **derived business attribute**:

```text
Customer historical city
          ↓
Configurable city → territory mapping
          ↓
Territory at order time
```

Example:

```text
Hyderabad ───────┐
Vijayawada ──────┼──> South Territory
Bengaluru ───────┘

Mumbai ──────────┐
Pune ────────────┼──> West Territory
Nashik ──────────┘
```

The city-to-territory mapping must be maintained as configuration rather than embedded in Spark code.

### Recommended configuration

`config/city_territory_map.csv`

```csv
city,territory
Hyderabad,SOUTH
Vijayawada,SOUTH
Bengaluru,SOUTH
Mumbai,WEST
Pune,WEST
```

The actual city/territory values should be replaced with the trainer-approved mapping.

### Critical historical rule

Territory must be derived from the customer's **historical city at order time**.

Correct:

```text
Order timestamp
       ↓
Customer SCD2 point-in-time lookup
       ↓
City at order
       ↓
City → Territory
       ↓
Territory at order
```

Incorrect:

```text
Order timestamp
       ↓
Current customer row
       ↓
Current city
       ↓
Current territory
```

This gives a real business reason for implementing **SCD Type 2**.

---

# 5. Why This Problem Fits the Capstone

The original capstone requires the platform to preserve source evidence, separate invalid data, process changes incrementally, maintain customer history, reconcile published totals, expose a small streaming flow, and orchestrate the process with Airflow. This project uses each of those capabilities to produce trusted territory analytics.

The core capstone scope explicitly includes PostgreSQL/Odoo sales, customer API, product CSV, inventory Excel, supplier XML, payment JSON, and a Kafka sales-event stream. fileciteturn0file0L53-L65

The project also retains the required distinction between customer SCD Type 2 and product SCD Type 1, with sales facts resolved to the customer version effective at the order timestamp. fileciteturn0file0L102-L120

---

# 6. End-to-End Architecture

```text
                          SOURCE SYSTEMS
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
 Odoo PostgreSQL          Customer API          Product CSV
        │                      │                      │
        ├──────────────────────┼──────────────────────┤
        │                      │                      │
 Inventory Excel        Supplier XML          Payment JSON
        │                      │                      │
        └──────────────────────┬──────────────────────┘
                               │
                          Kafka Events
                               │
                               ↓
                       ┌─────────────────┐
                       │ BRONZE - HDFS   │
                       │ Raw Evidence    │
                       └────────┬────────┘
                                ↓
                       ┌─────────────────┐
                       │ SILVER - Spark  │
                       │ Typed + Trusted │
                       └────────┬────────┘
                                │
             ┌──────────────────┼──────────────────┐
             ↓                  ↓                  ↓
        Validation        Deduplication       SCD History
             │                  │                  │
        Quarantine        Incremental        Customer SCD2
                           State Mgmt         Product SCD1
             └──────────────────┼──────────────────┘
                                ↓
                       ┌─────────────────┐
                       │ Quality Gate    │
                       └───────┬─────────┘
                               │
                   ┌───────────┴───────────┐
                   ↓                       ↓
                 PASS                    FAIL
                   ↓                       ↓
                 GOLD              Audit + Quarantine
                   ↓
             SNOWFLAKE
       DIMENSIONS + FACT SALES
                   ↓
        TERRITORY INTELLIGENCE
```

---

# 7. Technology Stack

| Process | Tool | Purpose |
|---|---|---|
| Source extraction | Python | API/database/file extraction |
| Odoo sales | PostgreSQL + Python connector | Read-only extraction from source views |
| Customer API | Python + provided adapter | Pagination and incremental updates |
| Product CSV | PySpark | Typed ingestion |
| Excel | Python/openpyxl or provided parser | Worksheet parsing |
| XML | Python XML tooling / provided helper | Supplier reference parsing |
| Payment JSON | PySpark | Explicit-schema validation |
| Streaming source | Apache Kafka | `sales-events` topic |
| Streaming engine | Spark Structured Streaming | Event ingestion/deduplication |
| Transformation engine | PySpark | Validation, joins, windows, aggregations |
| Lake | HDFS | Bronze/Silver/Gold storage |
| File format | Parquet | Columnar storage |
| Data quality | PySpark + audit/control tables | DQ rules and publication gate |
| Orchestration | Apache Airflow | Batch workflow, retry and recovery |
| Warehouse | Snowflake | Star schema and analytics |
| Warehouse loading | Snowflake connector / supplied local path | Local-to-Snowflake loading |
| Configuration | YAML/CSV | Paths, connections, city-territory mapping |
| Version control | Git | Reproducible project source |
| Optional cloud | AWS S3 + EMR | Portability demonstration |

---

# 8. All Capstone Concepts Demonstrated

| Capstone concept | Project implementation |
|---|---|
| Multi-source ingestion | PostgreSQL, API, CSV, Excel, XML, JSON, Kafka |
| Source traceability | `source_system`, `source_reference`, `batch_id`, ingestion timestamp |
| Bronze layer | Immutable raw evidence |
| Silver layer | Typed, standardized, validated state |
| Gold layer | Territory business analytics |
| HDFS | Lake persistence |
| Parquet | Physical storage |
| Explicit schemas | Spark schemas for core inputs |
| Data quality | Keys, timestamps, numeric ranges, references, duplicates |
| Quarantine | Invalid/malformed data with actionable reasons |
| Incremental processing | Pack 01, Pack 02, Pack 03 |
| Batch ledger / HWM | Successful source progress boundary |
| Deterministic deduplication | Business key + source version ordering |
| Duplicate handling | Exact replay detection |
| Supersession | Older valid source versions |
| Conflict handling | Same key + version conflicts surfaced |
| SCD Type 2 | Customer city and segment |
| SCD Type 1 | Product descriptive attributes |
| Point-in-time join | Order timestamp → customer historical version |
| Derived business mapping | Historical city → territory |
| Decimal money arithmetic | Line-level `quantity × price × discount` |
| Cancellation handling | State update, no duplicate sales line |
| Reconciliation | Row accounting + financial bridge |
| Quality publication gate | Fail batch before Gold/warehouse publication |
| Snowflake | Dimensions and sales fact |
| Window functions | SCD, lag, ranking, rolling metrics, concentration |
| Structured Streaming | Kafka event visibility |
| Persistent checkpoint | Consumer restart support |
| Streaming deduplication | `event_id` based |
| Late events | >10-minute lateness flag |
| Airflow | Parameterized end-to-end DAG |
| Retry/recovery | Controlled failure and resume |
| Idempotency | Replaying a completed pack leaves logical output unchanged |
| Auditability | Attempt IDs, counts, status, boundaries |
| Portability | Configuration-based local / cloud profile |
| Reproducibility | Exact execution and replay commands |

The capstone specifically requires Bronze, Silver, Gold and quarantine/audit areas, explicit schemas, validation, deterministic deduplication and update handling. fileciteturn0file0L82-L100

---

# 9. Business Entities in Scope

The core source entities are:

1. Customer
2. Product
3. Sales Order
4. Sales Order Line
5. Inventory Snapshot
6. Supplier Product Reference
7. Payment Event

Plus the Kafka `sales-events` stream.

The Excel, XML and payment JSON feeds are small interface exercises in the core capstone and do not need to become territory fact tables unless the optional inventory extension is selected. fileciteturn0file0L59-L65

---

# 10. Bronze Layer Design

Bronze is an **immutable source-evidence layer**.

Do not clean or overwrite source values in Bronze.

Every Bronze record or its manifest retains:

```text
source_system
source_reference
batch_id
ingestion_timestamp
raw_payload / raw-file reference
business keys
source timestamps
source version
```

The source reference for streaming records must include Kafka topic/partition/offset, while file and API sources must remain recoverable through their extraction/file references. fileciteturn0file0L94-L98

---

# 11. Bronze Schema — Sales Orders

## `bronze_sales_orders`

**Source:** Odoo PostgreSQL read-only sales view

| Column | Type | Required | Purpose |
|---|---|---:|---|
| `order_id` | STRING | Yes | Stable order business key |
| `customer_id` | STRING | Yes | Customer business key |
| `order_ts` | TIMESTAMP | Yes | Order business timestamp |
| `order_status` | STRING | Yes | Draft / confirmed / cancelled |
| `currency` | STRING | Yes | Expected INR |
| `source_modified_ts` | TIMESTAMP | Yes | Source modification time |
| `source_version` | STRING/LONG | Yes | Deterministic ordering |
| `source_system` | STRING | Yes | Source lineage |
| `source_reference` | STRING | Yes | Extraction reference |
| `batch_id` | STRING | Yes | Release ID |
| `ingestion_timestamp` | TIMESTAMP | Yes | Ingestion time |
| `raw_payload` | STRING | Useful | Raw source record |

### Business grain

```text
order_id
```

---

# 12. Bronze Schema — Sales Order Lines

## `bronze_sales_order_lines`

**Source:** Odoo PostgreSQL sales view

| Column | Type | Required | Purpose |
|---|---|---:|---|
| `order_id` | STRING | Yes | Parent order |
| `line_id` | STRING | Yes | Line identifier |
| `product_id` | STRING | Yes | Product business key |
| `quantity` | DECIMAL(18,3) | Yes | Ordered quantity |
| `unit_price` | DECIMAL(18,2) | Yes | Unit price |
| `discount_percentage` | DECIMAL(5,2) | Yes | Discount percentage |
| `source_modified_ts` | TIMESTAMP | Yes | Source modification time |
| `source_version` | STRING/LONG | Yes | Deterministic ordering |
| `source_system` | STRING | Yes | Source lineage |
| `source_reference` | STRING | Yes | Extraction reference |
| `batch_id` | STRING | Yes | Release ID |
| `ingestion_timestamp` | TIMESTAMP | Yes | Ingestion time |
| `raw_payload` | STRING | Useful | Raw source record |

### Business grain

```text
(order_id, line_id)
```

This is also the required analytical fact grain. fileciteturn0file0L69-L78

---

# 13. Bronze Schema — Customers

## `bronze_customers`

**Source:** Trainer-provided HTTP API/adapter

| Column | Type | Required | Purpose |
|---|---|---:|---|
| `customer_id` | STRING | Yes | Stable source ID |
| `customer_name` | STRING | Yes | Customer name |
| `city` | STRING | Yes | Geographic attribute |
| `segment` | STRING | Yes | Customer segment |
| `business_effective_ts` | TIMESTAMP | Yes | Effective change time |
| `source_modified_ts` | TIMESTAMP | Yes | Source update time |
| `source_version` | STRING/LONG | Yes | Deterministic version |
| `source_system` | STRING | Yes | Source lineage |
| `source_reference` | STRING | Yes | API/extraction reference |
| `batch_id` | STRING | Yes | Batch/release |
| `ingestion_timestamp` | TIMESTAMP | Yes | Ingestion time |
| `raw_payload` | STRING | Useful | Original API record |

Customer city and segment become SCD Type 2 attributes.

---

# 14. Bronze Schema — Products

## `bronze_products`

**Source:** Product CSV

| Column | Type | Required | Purpose |
|---|---|---:|---|
| `product_id` | STRING | Yes | Stable product ID |
| `product_name` | STRING | Yes | Product description |
| `category` | STRING | Yes | Product category |
| `source_modified_ts` | TIMESTAMP | Yes | Source update time |
| `source_version` | STRING/LONG | Yes | Deterministic version |
| `source_system` | STRING | Yes | Source lineage |
| `source_reference` | STRING | Yes | File reference |
| `batch_id` | STRING | Yes | Batch |
| `ingestion_timestamp` | TIMESTAMP | Yes | Ingestion time |
| `raw_payload` | STRING | Useful | Raw record |

Product name/category are SCD Type 1 attributes.

---

# 15. Bronze Schema — Inventory

## `bronze_inventory_snapshots`

**Source:** Excel

| Column | Type | Required | Purpose |
|---|---|---:|---|
| `snapshot_date` | DATE | Yes | Snapshot date |
| `product_id` | STRING | Yes | Product |
| `warehouse_id` | STRING | Yes | Warehouse |
| `stock_qty` | DECIMAL(18,3) | Yes | Stock quantity |
| `file_name` | STRING | Yes | Original workbook |
| `worksheet_name` | STRING | Useful | Worksheet |
| `batch_id` | STRING | Yes | Batch |
| `ingestion_timestamp` | TIMESTAMP | Yes | Ingestion time |

This is validated and reported in the core implementation but not joined into territory sales unless the optional inventory extension is chosen.

---

# 16. Bronze Schema — Supplier Product References

## `bronze_supplier_product_refs`

**Source:** XML

| Column | Type | Required | Purpose |
|---|---|---:|---|
| `supplier_id` | STRING | Yes | Supplier ID |
| `supplier_product_id` | STRING | Yes | Supplier product key |
| `product_id` | STRING | Useful | Internal product ID if supplied |
| `supplier_product_name` | STRING | Yes | Supplier product name |
| `supplier_category` | STRING | Useful | Supplier category |
| `reference_valid_from` | TIMESTAMP | Useful | Validity timestamp |
| `raw_xml_reference` | STRING | Yes | Recoverable location |
| `batch_id` | STRING | Yes | Batch |
| `ingestion_timestamp` | TIMESTAMP | Yes | Ingestion time |

---

# 17. Bronze Schema — Payment Events

## `bronze_payment_events`

**Source:** Payment gateway JSON

| Column | Type | Required | Purpose |
|---|---|---:|---|
| `payment_event_id` | STRING | Yes | Payment event identifier |
| `order_id` | STRING | Useful | Order reference |
| `event_type` | STRING | Yes | Payment event type |
| `event_ts` | TIMESTAMP | Yes | Event time if parseable |
| `amount` | DECIMAL(18,2) | Useful | Event amount |
| `currency` | STRING | Useful | Expected INR |
| `raw_payload` | STRING | Yes | Original JSON |
| `batch_id` | STRING | Yes | Batch |
| `ingestion_timestamp` | TIMESTAMP | Yes | Ingestion time |

Payment is not treated as recognized revenue or collected-sales fact data because the capstone explicitly defines net sales as ordered sales value. fileciteturn0file0L21-L27

---

# 18. Bronze Schema — Kafka Sales Events

## `bronze_sales_events`

**Source:** Kafka topic `sales-events`

| Column | Type | Required | Purpose |
|---|---|---:|---|
| `event_id` | STRING | Yes | Event deduplication key |
| `order_id` | STRING | Useful | Related order |
| `event_type` | STRING | Yes | Event type |
| `event_time` | TIMESTAMP | Yes | Business event time |
| `event_payload` | STRING | Yes | Raw Kafka payload |
| `processing_timestamp` | TIMESTAMP | Yes | Processing time |
| `topic` | STRING | Yes | Kafka topic |
| `partition` | INT | Yes | Kafka partition |
| `offset` | LONG | Yes | Kafka offset |
| `batch_id` | STRING | Yes | Replay/fixture context |

---

# 19. Bronze Storage Layout

Recommended:

```text
bronze/
├── sales_orders/
│   └── batch_id=<batch_id>/
├── sales_order_lines/
│   └── batch_id=<batch_id>/
├── customers/
│   └── batch_id=<batch_id>/
├── products/
│   └── batch_id=<batch_id>/
├── inventory_snapshots/
│   └── batch_id=<batch_id>/
├── supplier_product_refs/
│   └── batch_id=<batch_id>/
├── payment_events/
│   └── batch_id=<batch_id>/
└── sales_events/
    └── event_date=<date>/
```

---

# 20. Silver Layer — Purpose

Silver is the **trusted, typed, standardized and incrementally maintained** layer.

Main transformation responsibilities:

```text
Bronze
  ↓
Schema enforcement
  ↓
Type casting
  ↓
Timestamp normalization
  ↓
Validation
  ↓
Quarantine
  ↓
Deterministic deduplication
  ↓
Incremental current-state handling
  ↓
Customer SCD2
  ↓
Product SCD1
  ↓
Point-in-time customer lookup
  ↓
Territory mapping
  ↓
Fact-ready sales dataset
```

---

# 21. Silver Transformation 1 — Explicit Schema

Use **PySpark** with explicit schemas.

Do not depend on schema inference for the core datasets.

Normalize:

```text
timestamps → UTC
money → DECIMAL
quantity → DECIMAL
discount → DECIMAL
IDs → STRING
```

Standardize order statuses to:

```text
draft
confirmed
cancelled
```

---

# 22. Silver Transformation 2 — Sales Order Validation

Validate:

```text
order_id IS NOT NULL
customer_id IS NOT NULL
order_ts IS NOT NULL
order_status IS NOT NULL
order_status IN ('draft','confirmed','cancelled')
```

Invalid rows go to quarantine with:

```text
error_code
error_description
batch_id
source_reference
raw_payload
```

---

# 23. Silver Transformation 3 — Sales Line Validation

Rules:

```text
order_id IS NOT NULL
line_id IS NOT NULL
product_id IS NOT NULL

quantity > 0

unit_price >= 0

discount_percentage BETWEEN 0 AND 100
```

Missing price is invalid.

Zero price is valid.

These rules are directly defined by the capstone business rules. fileciteturn0file0L69-L80

---

# 24. Silver Transformation 4 — Line Net Value

Calculate with decimal arithmetic:

```text
line_net_value =
quantity
× unit_price
× (1 - discount_percentage / 100)
```

Round **each line** to two decimal places before aggregation.

Example:

```text
quantity = 5
unit_price = 1200
discount = 10%

5 × 1200 × 0.90
= 5400.00
```

This ensures territory totals reconcile exactly with Gold and Snowflake.

---

# 25. Silver Transformation 5 — Deterministic Deduplication

Use **PySpark Window functions**.

For order lines:

```text
PARTITION BY (order_id, line_id)
ORDER BY source_version DESC,
         source_modified_ts DESC
```

For customers:

```text
PARTITION BY customer_id
ORDER BY business_effective_ts,
         source_version
```

Classify incoming records:

```text
EXACT REPLAY
SUPERSEDED
ACCEPTED CHANGE
CONFLICT
```

Do not use ingestion time as the business-version selection rule.

For equal modification timestamps, the supplied source version/tie-break field must be used so the extraction boundary cannot lose records.

---

# 26. Silver Transformation 6 — Incremental Processing

Process:

```text
Pack 01 → Initial Load
Pack 02 → Changes
Pack 03 → Changes
```

A stable batch ledger records:

```text
attempt_id
batch_id
source_system
start_time
end_time
status
records_read
accepted_records
rejected_records
duplicate_records
superseded_records
last_successful_boundary
```

Only advance the source high-water mark when:

```text
required publication
+
reconciliation
+
quality gate
```

all succeed.

This prevents a failed batch from moving the progress boundary.

The capstone explicitly requires successful progress to advance only after required publication and reconciliation steps succeed. fileciteturn0file0L104-L114

---

# 27. Silver Transformation 7 — Customer SCD Type 2

## Why it matters

Customer geography changes.

Example:

```text
Customer C101

2026-01-01 → Hyderabad
2026-06-01 → Mumbai
```

An order on:

```text
2026-03-15
```

must remain associated with:

```text
Hyderabad
```

and therefore the territory mapped from Hyderabad.

### SCD2 Columns

```text
customer_sk
customer_id
customer_name
city
segment
effective_from
effective_to
is_current
```

### Interval semantics

Use:

```text
[effective_from, effective_to)
```

Therefore:

```text
effective_from <= order_ts
AND order_ts < effective_to
```

Use Spark window functions such as:

```text
LAG()
LEAD()
ROW_NUMBER()
```

to identify changes and construct intervals.

Unchanged replays must not generate new customer versions.

Customer intervals must not overlap.

Exactly one row per customer should be current.

These requirements are directly aligned to the capstone's customer-history specification. fileciteturn0file0L114-L120

---

# 28. Silver Transformation 8 — Product SCD Type 1

Products use Type 1.

For every `product_id`:

```text
latest accepted version
        ↓
overwrite product_name/category
```

Historical product reporting therefore uses the latest product description/category.

This intentionally contrasts with the historical customer treatment.

---

# 29. Silver Transformation 9 — Point-in-Time Customer Join

For each sales order:

```text
sales.customer_id = customer.customer_id
AND sales.order_ts >= customer.effective_from
AND sales.order_ts < customer.effective_to
```

Output:

```text
customer_sk
customer_id
city_at_order
segment_at_order
```

This is critical.

The territory is based on:

```text
city_at_order
```

not current customer city.

---

# 30. Silver Transformation 10 — City to Territory Mapping

Join the point-in-time customer result to the configuration mapping:

```text
city_at_order
      =
cfg_city_territory_map.city
```

Output:

```text
territory_at_order
```

Recommended Silver mapping dataset:

## `silver_city_territory_map`

| Column | Type |
|---|---|
| `city` | STRING |
| `territory` | STRING |
| `mapping_version` | STRING |
| `active_flag` | BOOLEAN |

Unknown cities should be treated as a **reference-quality failure** for the territory analytics candidate rather than silently assigned to an arbitrary territory.

---

# 31. Silver Transformation 11 — Fact Preparation

Build the trusted latest sales-line state:

```text
Silver Orders
       +
Silver Order Lines
       +
Customer SCD2 point-in-time lookup
       +
Product SCD1
       +
City → Territory mapping
       +
DIM_DATE mapping
```

Output fields include:

```text
order_id
line_id
customer_id
customer_sk
product_id
product_sk
order_ts
order_date
status
quantity
unit_price
discount_percentage
line_net_value
city_at_order
segment_at_order
territory_at_order
source_version
```

---

# 32. Silver Transformation 12 — Cancellation Handling

If an order is cancelled:

```text
existing order state → cancelled
```

Do not create a new sales line.

Cancelled lines remain traceable but are excluded from eligible sales measures.

Therefore:

```text
FACT_SALES
    ├── confirmed → KPI eligible
    ├── draft     → not KPI eligible
    └── cancelled → not KPI eligible
```

---

# 33. Silver Transformation 13 — Quarantine

Every quarantined row contains:

```text
raw_payload / recoverable source location
source_system
source_reference
batch_id
error_code
error_description
ingestion_timestamp
```

Recommended error codes:

| Error Code | Meaning |
|---|---|
| `MISSING_KEY` | Required key missing |
| `INVALID_TIMESTAMP` | Timestamp cannot be parsed |
| `INVALID_QUANTITY` | Quantity <= 0 |
| `INVALID_PRICE` | Missing or negative price |
| `INVALID_DISCOUNT` | Discount outside 0–100 |
| `UNKNOWN_CUSTOMER` | Customer reference missing |
| `UNKNOWN_PRODUCT` | Product reference missing |
| `UNKNOWN_CITY_TERRITORY` | City cannot be mapped to territory |
| `DUPLICATE_REPLAY` | Exact duplicate |
| `SUPERSEDED_VERSION` | Older source version |
| `SOURCE_CONFLICT` | Same key/version conflict |
| `PARSE_ERROR` | Malformed input |

---

# 34. Snowflake Warehouse

The required star schema remains:

```text
DIM_CUSTOMER
DIM_PRODUCT
DIM_DATE
FACT_SALES
```

Territory does not require a new upstream entity or mandatory warehouse dimension.

Territory is derived from the historical customer city.

The capstone explicitly requires these four tables and a sales-line fact grain. fileciteturn0file0L122-L135

---

# 35. Snowflake — DIM_CUSTOMER

## Grain

One row per historical customer version.

| Column | Type | Description |
|---|---|---|
| `customer_sk` | NUMBER | Surrogate key |
| `customer_id` | VARCHAR | Business key |
| `customer_name` | VARCHAR | Customer name |
| `city` | VARCHAR | Historical city |
| `segment` | VARCHAR | Historical segment |
| `effective_from` | TIMESTAMP | Version start |
| `effective_to` | TIMESTAMP | Version end |
| `is_current` | BOOLEAN | Current-row flag |

---

# 36. Snowflake — DIM_PRODUCT

## Grain

One row per product.

| Column | Type |
|---|---|
| `product_sk` | NUMBER |
| `product_id` | VARCHAR |
| `product_name` | VARCHAR |
| `category` | VARCHAR |
| `updated_at` | TIMESTAMP |

SCD Type 1.

---

# 37. Snowflake — DIM_DATE

## Grain

One row per required reporting date.

| Column | Type |
|---|---|
| `date_sk` | NUMBER |
| `calendar_date` | DATE |
| `day_of_week` | NUMBER |
| `month` | NUMBER |
| `month_name` | VARCHAR |
| `quarter` | NUMBER |
| `year` | NUMBER |

---

# 38. Snowflake — FACT_SALES

## Grain

One row per latest accepted sales-order line.

| Column | Type | Description |
|---|---|---|
| `sales_sk` | NUMBER | Fact surrogate key |
| `order_id` | VARCHAR | Order ID |
| `line_id` | VARCHAR | Line ID |
| `customer_sk` | NUMBER | Historical customer FK |
| `product_sk` | NUMBER | Product FK |
| `date_sk` | NUMBER | Date FK |
| `order_ts` | TIMESTAMP | Order timestamp |
| `order_date` | DATE | UTC order date |
| `status` | VARCHAR | Latest status |
| `quantity` | DECIMAL(18,3) | Quantity |
| `unit_price` | DECIMAL(18,2) | Unit price |
| `discount_percentage` | DECIMAL(5,2) | Discount |
| `line_net_value` | DECIMAL(18,2) | Rounded net line amount |
| `source_version` | VARCHAR | Source ordering evidence |

---

# 39. Gold Layer Overview

Gold is where the business meaning of the project becomes visible.

```text
                   GOLD
                     │
       ┌─────────────┼──────────────┐
       ↓             ↓              ↓
   TERRITORY       PRODUCT       CUSTOMER
    SALES         ANALYTICS      ANALYTICS
       │             │              │
       └─────────────┼──────────────┘
                     ↓
              TERRITORY TRENDS
                     ↓
             BUSINESS QUESTIONS
```

---

# 40. Gold Table 1 — Territory Daily Sales

## `GOLD_TERRITORY_DAILY_SALES`

### Grain

```text
order_date + territory
```

| Column | Type | Description |
|---|---|---|
| `order_date` | DATE | UTC reporting date |
| `territory` | VARCHAR | Derived territory |
| `confirmed_orders` | BIGINT | Eligible order count |
| `cancelled_orders` | BIGINT | Cancelled order count |
| `unique_customers` | BIGINT | Distinct customers |
| `units_sold` | DECIMAL(18,3) | Units |
| `net_sales` | DECIMAL(18,2) | Net sales |
| `avg_order_value` | DECIMAL(18,2) | Sales / distinct eligible orders |
| `sales_share_pct` | DECIMAL(8,4) | Share of daily total |

---

# 41. Gold Table 2 — Territory Monthly Performance

## `GOLD_TERRITORY_MONTHLY_PERFORMANCE`

### Grain

```text
year_month + territory
```

| Column | Type |
|---|---|
| `year_month` | DATE / STRING |
| `territory` | VARCHAR |
| `customers` | BIGINT |
| `orders` | BIGINT |
| `units_sold` | DECIMAL(18,3) |
| `net_sales` | DECIMAL(18,2) |
| `avg_order_value` | DECIMAL(18,2) |
| `previous_month_sales` | DECIMAL(18,2) |
| `sales_change_value` | DECIMAL(18,2) |
| `sales_change_pct` | DECIMAL(9,4) |
| `territory_rank` | INT |

---

# 42. Gold Table 3 — Territory Product Performance

## `GOLD_TERRITORY_PRODUCT_PERFORMANCE`

### Grain

```text
order_date + territory + product_id
```

| Column | Type |
|---|---|
| `order_date` | DATE |
| `territory` | VARCHAR |
| `product_id` | VARCHAR |
| `product_name` | VARCHAR |
| `category` | VARCHAR |
| `orders` | BIGINT |
| `units_sold` | DECIMAL(18,3) |
| `net_sales` | DECIMAL(18,2) |
| `sales_contribution_pct` | DECIMAL(8,4) |
| `product_rank_in_territory` | INT |

---

# 43. Gold Table 4 — Territory Segment Performance

## `GOLD_TERRITORY_SEGMENT_PERFORMANCE`

### Grain

```text
order_date + territory + segment_at_order
```

| Column | Type |
|---|---|
| `order_date` | DATE |
| `territory` | VARCHAR |
| `segment_at_order` | VARCHAR |
| `customers` | BIGINT |
| `orders` | BIGINT |
| `units_sold` | DECIMAL(18,3) |
| `net_sales` | DECIMAL(18,2) |
| `avg_order_value` | DECIMAL(18,2) |
| `segment_sales_share_pct` | DECIMAL(8,4) |

---

# 44. Gold Table 5 — Territory Customer Performance

## `GOLD_TERRITORY_CUSTOMER_PERFORMANCE`

### Grain

```text
territory + customer_id
```

| Column | Type |
|---|---|
| `territory` | VARCHAR |
| `customer_id` | VARCHAR |
| `customer_name` | VARCHAR |
| `city_at_order` | VARCHAR |
| `segment_at_order` | VARCHAR |
| `orders` | BIGINT |
| `units_sold` | DECIMAL(18,3) |
| `net_sales` | DECIMAL(18,2) |
| `customer_sales_share_pct` | DECIMAL(8,4) |
| `customer_rank_in_territory` | INT |

---

# 45. Gold Table 6 — Territory Trends

## `GOLD_TERRITORY_TRENDS`

### Grain

```text
month + territory
```

| Column | Type |
|---|---|
| `year_month` | DATE / STRING |
| `territory` | VARCHAR |
| `net_sales` | DECIMAL(18,2) |
| `previous_period_sales` | DECIMAL(18,2) |
| `sales_growth_pct` | DECIMAL(9,4) |
| `rolling_3_month_sales` | DECIMAL(18,2) |
| `orders` | BIGINT |
| `previous_period_orders` | BIGINT |
| `order_growth_pct` | DECIMAL(9,4) |
| `aov` | DECIMAL(18,2) |
| `previous_period_aov` | DECIMAL(18,2) |
| `aov_change_pct` | DECIMAL(9,4) |
| `territory_rank` | INT |
| `territory_status` | VARCHAR |

Example statuses:

```text
GROWING
STABLE
DECLINING
```

The threshold should be configuration-driven.

---

# 46. Gold Table 7 — Territory Data Quality

## `GOLD_TERRITORY_DATA_QUALITY`

### Grain

```text
batch_id + source_system
```

| Column | Type |
|---|---|
| `batch_id` | VARCHAR |
| `source_system` | VARCHAR |
| `records_read` | BIGINT |
| `accepted_records` | BIGINT |
| `rejected_records` | BIGINT |
| `duplicate_records` | BIGINT |
| `superseded_records` | BIGINT |
| `rejection_rate_pct` | DECIMAL(9,4) |
| `quality_status` | VARCHAR |
| `batch_status` | VARCHAR |
| `last_successful_boundary` | VARCHAR |

This makes it possible to distinguish:

```text
Territory sales truly declined
```

from:

```text
Territory sales appears to have declined
because 12% of source records were rejected
```

---

# 47. Gold Table 8 — Streaming Event Activity

## `GOLD_TERRITORY_STREAM_ACTIVITY`

### Grain

```text
event_date + event_type
```

| Column | Type |
|---|---|
| `event_date` | DATE |
| `event_type` | VARCHAR |
| `distinct_valid_events` | BIGINT |
| `duplicate_replays` | BIGINT |
| `malformed_events` | BIGINT |
| `late_events` | BIGINT |
| `first_seen_ts` | TIMESTAMP |
| `last_seen_ts` | TIMESTAMP |

This provides event visibility without changing the authoritative batch sales fact.

---

# 48. Gold Mapping — Territory Daily Sales

```text
FACT_SALES
    │
    ├── customer_sk
    │      ↓
    │   DIM_CUSTOMER
    │      ↓
    │   historical city
    │
    ├── city → territory config
    │
    └── status = confirmed
           ↓
 GROUP BY order_date + territory
           ↓
GOLD_TERRITORY_DAILY_SALES
```

---

# 49. Gold Mapping — Territory Monthly Performance

```text
FACT_SALES
      ↓
DIM_CUSTOMER
      ↓
historical city / segment
      ↓
city → territory
      ↓
GROUP BY month + territory
      ↓
SUM sales / orders / units
      ↓
LAG sales
RANK territories
      ↓
GOLD_TERRITORY_MONTHLY_PERFORMANCE
```

---

# 50. Gold Mapping — Territory × Product

```text
FACT_SALES
      ├───────────────→ DIM_PRODUCT
      │                     ↓
      │               product/category
      │
      └──→ DIM_CUSTOMER
                 ↓
           historical city
                 ↓
         city → territory
                 ↓
GROUP BY date + territory + product
                 ↓
GOLD_TERRITORY_PRODUCT_PERFORMANCE
```

---

# 51. Gold Mapping — Territory × Segment

```text
FACT_SALES
      ↓
DIM_CUSTOMER
      ↓
historical segment
historical city
      ↓
city → territory
      ↓
GROUP BY territory + segment
      ↓
GOLD_TERRITORY_SEGMENT_PERFORMANCE
```

---

# 52. Gold Mapping — Territory × Customer

```text
FACT_SALES
      ↓
DIM_CUSTOMER
      ↓
historical city + segment
      ↓
city → territory
      ↓
GROUP BY territory + customer
      ↓
RANK customers inside territory
      ↓
GOLD_TERRITORY_CUSTOMER_PERFORMANCE
```

---

# 53. Gold Mapping — Territory Trends

Start with the trusted monthly territory dataset:

```text
GOLD_TERRITORY_MONTHLY_PERFORMANCE
                 ↓
            Window Functions
                 ↓
      ┌──────────┼──────────┐
      ↓          ↓          ↓
     LAG()     RANK()   Rolling SUM()
      ↓          ↓          ↓
         GOLD_TERRITORY_TRENDS
```

Example:

```text
sales_growth_pct =
(current_month_sales - previous_month_sales)
/
previous_month_sales
× 100
```

---

# 54. Window Functions

This project gives meaningful use cases for Spark window functions.

## SCD2

```text
ROW_NUMBER()
LAG()
LEAD()
```

## Territory growth

```text
LAG(net_sales)
```

## Territory ranking

```text
RANK()
DENSE_RANK()
```

## Rolling sales

```text
SUM(net_sales) OVER (
    PARTITION BY territory
    ORDER BY year_month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)
```

## Customer concentration

```text
SUM(customer_sales) OVER (
    PARTITION BY territory
    ORDER BY customer_sales DESC
)
```

This allows cumulative contribution calculations.

---

# 55. Territory Performance Metrics

## Sales Metrics

```text
Net Sales
Confirmed Orders
Units Sold
Average Order Value
Sales Share %
```

## Growth Metrics

```text
Month-over-Month Sales Growth
Order Growth
AOV Growth
Rolling 3-Month Sales
```

## Territory Composition

```text
Customer Count
Customer Segment Mix
Product Mix
Top Customer Contribution
Top Product Contribution
```

## Operational Context

```text
Cancellation Count
Cancellation Rate
Rejected Records
Duplicate Records
Superseded Records
Late Events
```

---

# 56. Territory Classification

A simple transparent rule can classify territory health.

Example:

```text
sales_growth_pct >= +15%
          ↓
       GROWING

-15% < sales_growth_pct < +15%
          ↓
        STABLE

sales_growth_pct <= -15%
          ↓
       DECLINING
```

Store:

```text
territory_status
```

inside `GOLD_TERRITORY_TRENDS`.

The exact threshold should be configurable.

---

# 57. Territory Concentration

One useful advanced metric is:

> **How dependent is a territory on a small number of customers?**

For each territory:

```text
customer sales
      ↓
ORDER BY sales DESC
      ↓
cumulative sales
      ↓
cumulative sales %
```

Example output:

| Territory | Top Customer Share | Top 5 Share |
|---|---:|---:|
| SOUTH | 18.2% | 48.5% |
| WEST | 31.7% | 71.4% |
| NORTH | 12.9% | 39.2% |

This can expose territories that look strong but are highly concentrated.

---

# 58. Territory Product Mix

Calculate:

```text
territory_sales
     ↓
product_sales
     ↓
product_sales / territory_sales
```

Example:

```text
SOUTH
 ├── Electronics 52%
 ├── Furniture   28%
 └── Accessories 20%
```

This supports questions such as:

> Which products are responsible for a territory's growth?

---

# 59. Territory Segment Mix

Use the historical customer segment:

```text
territory
   ↓
segment at order time
   ↓
sales
```

Example:

```text
WEST
 ├── Premium  61%
 ├── Standard 29%
 └── Basic    10%
```

Because the segment is historically resolved, the analysis remains correct even after the customer moves segments.

---

# 60. Data Quality Publication Gate

Before Gold publication:

```text
Required keys = zero nulls
Silver grain = zero duplicate logical keys
Fact grain = zero duplicate (order_id, line_id)
Customer FKs = all valid
Product FKs = all valid
Quantity > 0
Price >= 0
Discount = 0 to 100
Calculated amount = valid
Rejection rate <= 5% configurable
```

A row may be quarantined while valid rows continue if the batch still passes the gate.

If the gate fails:

```text
Bronze preserved
Quarantine preserved
Audit preserved
Batch FAILED
Gold NOT published
Warehouse business changes NOT published
```

The capstone explicitly defines these publication conditions and requires a deliberately failing excessive-rejection fixture. fileciteturn0file0L139-L151

---

# 61. Batch Accounting

For every source/batch:

```text
records_read
=
accepted_records
+
rejected_records
+
duplicate_records
+
superseded_records
```

The classification order must be documented so a record with multiple errors is counted once.

For unreadable files where record count cannot be established:

```text
file-level failure
+
record reconciliation incomplete
+
batch failure
```

Do not invent counts.

---

# 62. Financial Reconciliation

Do not compare raw delivered data directly with current-state warehouse totals because change packs can contain duplicate and superseded deliveries.

Correct bridge:

```text
Raw source delivery
        ↓
Latest valid business state
        ↓
Eligible confirmed orders
        ↓
Line-level monetary calculation
        ↓
Silver trusted sales
        ↓
Gold territory totals
        ↓
Snowflake FACT_SALES
```

Required result:

```text
Gold territory total
=
Snowflake equivalent total
```

with monetary difference:

```text
0.00
```

after applying the declared filters and rounding rules.

---

# 63. Incremental Release Design

## Pack 01

Contains:

```text
initial customers
initial products
initial orders
initial order lines
known bad records
```

Expected outcome:

```text
initial territory performance baseline
```

---

## Pack 02

Contains:

```text
new orders
customer city changes
customer segment changes
product corrections
duplicate deliveries
invalid references
```

Expected outcome:

```text
territory metrics update without rebuilding everything
```

---

## Pack 03

Contains:

```text
another customer change
late-arriving order
cancelled order
replayed records
```

Expected outcome:

```text
historical territory assignment remains correct
cancellation changes existing fact state
late order joins historical customer version
replay causes no duplicate business output
```

---

# 64. Example of Why SCD2 Matters

Assume:

```text
Customer C001

January 1
City = Hyderabad
Territory = SOUTH

June 1
City = Mumbai
Territory = WEST
```

Orders:

```text
March 10 → ₹10,000
July 10  → ₹15,000
```

Correct territory analytics:

```text
SOUTH = ₹10,000
WEST  = ₹15,000
```

A current-state join would incorrectly place both orders into WEST.

This is one of the strongest demonstrations in the project.

---

# 65. Streaming Requirement

Use:

```text
Kafka topic: sales-events
```

Pipeline:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Explicit schema
  ↓
Bronze payload + offsets
  ↓
Malformed quarantine
  ↓
event_id deduplication
  ↓
late-event classification
  ↓
deduplicated event landing
```

Requirements:

- Persistent checkpoint outside temporary process directories
- One accepted logical event per distinct valid `event_id`
- Replay after restart does not increase logical accepted events
- Malformed messages are quarantined
- Late valid events are retained
- `processing_timestamp - event_time > 10 minutes` → `is_late = true`
- Test events should reach the lake within 60 seconds
- Restart consumer using the same checkpoint
- Event stream does not add amounts to `FACT_SALES`

The capstone explicitly makes Odoo batch data authoritative for `FACT_SALES`; the Kafka pipeline is for event visibility. fileciteturn0file0L161-L171

---

# 66. Airflow Orchestration

Create one parameterized DAG.

Recommended dependency graph:

```text
START
  ↓
CREATE_BATCH
  ↓
┌──────────────┬──────────────┬──────────────┬─────────────┐
│ Odoo Sales   │ Customer API │ Product CSV  │ Interface   │
└──────┬───────┴──────┬───────┴──────┬───────┴─────────────┘
       ↓              ↓              ↓
                 BRONZE LANDING
                       ↓
             SILVER TRANSFORMATIONS
                       ↓
          CUSTOMER SCD2 / PRODUCT SCD1
                       ↓
            CUSTOMER HISTORY RESOLUTION
                       ↓
             CITY → TERRITORY MAPPING
                       ↓
                  QUALITY GATE
                       ↓
                 GOLD BUILD
                       ↓
          LOAD SNOWFLAKE DIMENSIONS
                       ↓
             LOAD FACT_SALES
                       ↓
                RECONCILIATION
                       ↓
                    SUCCESS
```

Airflow must provide:

```text
parameterization
retries
dependencies
failure logs
batch rerun
non-overlapping publication attempts
```

---

# 67. Controlled Failure and Recovery

Inject a controlled failure:

```text
after intermediate Silver write
before successful publication
```

Expected:

```text
Batch = FAILED
Bronze = preserved
Silver = preserved
Quarantine = preserved
Audit = preserved
Gold = not published
```

Then resume using the same batch.

Expected:

```text
No duplicate Silver keys
No duplicate customer versions
No duplicate facts
No incorrect territory assignment
Same Gold totals as clean execution
Same financial totals
```

The capstone explicitly requires demonstrating this recovery behavior. fileciteturn0file0L175-L181

---

# 68. Business Questions

## Core Questions

1. What is the net value of confirmed sales orders by day and month?
2. Which products and categories generate the highest net sales value?
3. Who are the top customers, and how do sales vary by historical customer segment and city?
4. What is the average order value for each day?
5. How many source records were rejected, superseded, or identified as duplicates, and why?

## Territory Questions

6. Which territories generate the highest net sales?

7. Which territories are growing or declining month over month?

8. Which products and categories drive each territory's sales?

9. Which customer segments contribute most to each territory?

10. Which customers contribute most to each territory?

11. Which territory has the highest average order value?

12. Which territories have the highest cancellation rates?

13. How concentrated is each territory's customer base?

14. How did territory performance change between Pack 01, Pack 02 and Pack 03?

15. Is an apparent territory decline caused by real business movement or poor source data quality?

---

# 69. Example Analytics Story

A strong final result should support a narrative such as:

```text
WEST territory sales declined 18%
              ↓
Electronics category declined 31%
              ↓
Product P102 responsible for 55% of the decline
              ↓
Premium customer segment declined 22%
              ↓
Top 3 customers account for 61% of territory sales
              ↓
Data-quality rate = healthy
              ↓
Conclusion:
real business decline, not a pipeline-quality artifact
```

This is much stronger than simply presenting:

```text
WEST sales = ₹8.2M
```

---

# 70. Repository Structure

```text
sales-territory-intelligence/
│
├── airflow/
│   └── dags/
│       └── sales_territory_pipeline.py
│
├── config/
│   ├── local.yaml
│   ├── aws_emr.yaml
│   └── city_territory_map.csv
│
├── ingestion/
│   ├── odoo_sales.py
│   ├── customer_api.py
│   ├── product_csv.py
│   ├── inventory_excel.py
│   ├── supplier_xml.py
│   ├── payment_json.py
│   └── kafka_stream.py
│
├── spark/
│   ├── bronze/
│   ├── silver/
│   │   ├── orders.py
│   │   ├── order_lines.py
│   │   ├── customer_scd2.py
│   │   ├── product_scd1.py
│   │   ├── territory_mapping.py
│   │   └── fact_preparation.py
│   │
│   ├── gold/
│   │   ├── territory_daily.py
│   │   ├── territory_monthly.py
│   │   ├── territory_product.py
│   │   ├── territory_segment.py
│   │   ├── territory_customer.py
│   │   └── territory_trends.py
│   │
│   ├── quality/
│   └── reconciliation/
│
├── snowflake/
│   ├── ddl/
│   │   ├── dim_customer.sql
│   │   ├── dim_product.sql
│   │   ├── dim_date.sql
│   │   └── fact_sales.sql
│   └── sql/
│       └── analytical_queries.sql
│
├── tests/
├── evidence/
├── requirements.txt
└── README.md
```

---

# 71. Detailed Tool-to-Process Mapping

| Process | Tool |
|---|---|
| Batch parameter creation | Airflow + Python |
| Odoo sales extraction | PostgreSQL + Python |
| Customer API extraction | Python |
| Product CSV ingestion | PySpark |
| Excel parsing | Python/openpyxl or provided helper |
| XML parsing | Python XML / provided helper |
| Payment JSON | PySpark |
| Kafka ingestion | Spark Structured Streaming |
| Bronze persistence | HDFS + Parquet |
| Explicit schema | PySpark |
| Standardization | PySpark |
| Timestamp normalization | PySpark |
| Business validation | PySpark |
| Quarantine | PySpark + HDFS |
| Duplicate detection | PySpark Window |
| Supersession detection | PySpark Window |
| Customer SCD2 | PySpark Window |
| Product SCD1 | PySpark |
| Point-in-time join | PySpark |
| City-territory mapping | PySpark join |
| Daily/monthly aggregation | PySpark |
| Territory ranking | PySpark Window |
| Territory trends | PySpark Window |
| Customer concentration | PySpark Window |
| Audit ledger | Python/Spark + control tables |
| Financial reconciliation | PySpark + Snowflake SQL |
| Gold storage | HDFS + Parquet |
| Snowflake dimensions | Snowflake SQL |
| Snowflake fact | Snowflake SQL / connector |
| Analytical queries | Snowflake SQL |
| Orchestration | Airflow |
| Retry | Airflow |
| Recovery | Airflow + idempotent Spark logic |
| Streaming checkpoint | Spark Structured Streaming |
| Streaming deduplication | Spark Structured Streaming |
| Version control | Git |
| Optional cloud | AWS S3 + EMR |

---

# 72. Acceptance Criteria Coverage

| Capstone AC | Territory Project Demonstration |
|---|---|
| AC01 — Core ingestion | PostgreSQL, API, CSV, Excel, XML, JSON, Kafka |
| AC02 — Layer separation | Bronze/Silver/Gold + quarantine |
| AC03 — DQ gate | All validation rules + configurable rejection threshold |
| AC04 — Incremental changes | Pack 01/02/03 update territory analytics |
| AC05 — Idempotency | Replay produces unchanged logical territory outputs |
| AC06 — Customer/product history | SCD2 customer city/segment + SCD1 product |
| AC07 — Warehouse correctness | Correct dimensions, fact grain and FKs |
| AC08 — Business answers | Core + territory-specific analytical SQL |
| AC09 — Reconciliation | Source accounting + Gold/Snowflake financial reconciliation |
| AC10 — Streaming | Kafka event visibility + replay/restart/late events |
| AC11 — Orchestration/recovery | Airflow DAG + injected failure + successful recovery |
| AC12 — Reproducibility | Configuration-driven local workflow |

The acceptance criteria explicitly require incremental correctness, idempotency, customer/product history, warehouse correctness, business answers, reconciliation, streaming and Airflow recovery. fileciteturn0file0L197-L209

---

# 73. Evidence Checklist

The final evidence report should include:

## Source ingestion

```text
source name
batch id
records read
```

## Data quality

```text
accepted
rejected
duplicates
superseded
rejection rate
```

## Customer history

Example:

```text
Customer C001
Before: Hyderabad → SOUTH
After : Mumbai    → WEST
```

Then prove:

```text
Old order → SOUTH
New order → WEST
```

## Territory performance

Show:

```text
Top territory
Fastest-growing territory
Declining territory
Top product by territory
Top segment by territory
Top customer by territory
Customer concentration
```

## Reconciliation

```text
Source eligible total
Silver total
Gold total
Snowflake total
Difference = 0.00
```

## Streaming

Demonstrate:

```text
valid event
duplicate replay
malformed event
late event
restart
```

## Recovery

Demonstrate:

```text
injected failure
     ↓
resume same batch
     ↓
no duplicate business output
     ↓
same final totals as clean run
```

---

# 74. Optional Extension

After the core acceptance criteria pass, choose **at most one** optional extension.

### Recommended for this project: Inventory Analytics

Use the supplied Excel inventory snapshots and supplier product references to answer one clearly defined stock-availability question, for example:

> Which territories have high sales demand but weak stock availability for the products driving that demand?

Possible extension:

```text
Territory Sales
      +
Product Stock
      +
Supplier Reference
      ↓
GOLD_TERRITORY_STOCK_PRESSURE
```

Possible schema:

| Column | Type |
|---|---|
| `snapshot_date` | DATE |
| `territory` | VARCHAR |
| `product_id` | VARCHAR |
| `net_sales` | DECIMAL(18,2) |
| `stock_qty` | DECIMAL(18,3) |
| `sales_per_stock_unit` | DECIMAL(18,4) |
| `stock_pressure_flag` | VARCHAR |

This should be implemented only after the core submission is stable.

---

# 75. Scope Guardrails

Not required:

- Production CDC administration
- Real-time warehouse serving
- Dashboard application
- Invoice modelling
- Payment allocation
- Hard-delete handling
- Retroactive SCD repair
- Complex ML-based territory forecasting
- Replacing Odoo or its PostgreSQL system
- Building a separate territory master application

The core objective is:

> **Transform changing multi-source operational data into trustworthy, historically correct territory intelligence.**

---

# 76. Final Project Outcome

At the end of the project, management should be able to answer:

> **Which territory performs best?**

> **Which territories are growing or declining?**

> **Which products and categories drive each territory?**

> **Which customer segments drive each territory?**

> **Which customers contribute most to each territory?**

> **How concentrated is each territory's sales base?**

> **How did territory performance change after incremental source updates?**

> **Are the territory numbers historically correct and financially reconciled?**

The engineering foundation behind these answers demonstrates:

```text
Python
PostgreSQL
Customer API
CSV
Excel
XML
JSON
Kafka
PySpark
HDFS
Parquet
Data Quality
Quarantine
Incremental Processing
Deduplication
SCD Type 2
SCD Type 1
Window Functions
Airflow
Snowflake
Reconciliation
Idempotency
Failure Recovery
Configuration / Portability
```

