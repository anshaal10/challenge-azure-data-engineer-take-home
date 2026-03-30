# The Blue Owls Solutions — Azure Data Engineer Take-Home Assessment

| | |
|---|---|
| **Tech Stack** | Python, PySpark, SQL, Jupyter Lab |
| **Deliverable** | Public GitHub repository |
| **Data Source** | Blue Owls Data API (credentials provided separately) |
| **Expected Time** | 8-12 hours |

---

> **Before you begin:** Read [GETTING_STARTED.md](GETTING_STARTED.md) for environment setup instructions.

---

## Overview

You are a data engineer joining a retail analytics team at Blue Owls Solutions. A partner company has exposed their e-commerce data through a set of internal APIs. Your job is to build a data pipeline that ingests this data, transforms it through a medallion architecture, and produces a star schema for analytics.

The underlying data is based on the Olist Brazilian E-Commerce dataset and is served through our data API. The API is intentionally imperfect — it simulates real-world conditions including intermittent failures, authentication challenges, and occasional data quality issues.

We are not looking for perfection. We are looking for someone who builds resilient systems, thinks clearly about data, and can articulate their decisions. A pipeline that handles failures gracefully and is clearly reasoned is worth more to us than one that is technically complete but brittle or opaque.

---

## Data Access

The data is available through the Blue Owls Data API at `{base_url}/api/v1/data`.

**Authentication:** All requests require a Bearer token. Obtain one by calling the `/api/v1/auth/token` endpoint with the credentials provided in your welcome email.

### Available Endpoints

- `GET /api/v1/data/orders`
- `GET /api/v1/data/order_items`
- `GET /api/v1/data/customers`
- `GET /api/v1/data/products`
- `GET /api/v1/data/sellers`
- `GET /api/v1/data/payments`

Each endpoint supports pagination via `?page=1&page_size=1000`. Responses are returned as JSON arrays.

The `orders` and `order_items` endpoints also support date filtering via `?date_from=YYYY-MM-DD&date_to=YYYY-MM-DD`.

> **📅 Data scope:** Ingest records from **2018-07-01 onward** across all endpoints. Use the `date_from` parameter where supported, and apply the same cutoff when filtering related tables (e.g. payments, customers) by joining on order date.

> **⚠️ Important:** The API is intentionally imperfect. It may return intermittent 500 errors, token expiration responses (401), rate limiting (429), and occasional malformed or incomplete records in the payload. Your pipeline should handle these gracefully.

---

## Part 1 — Data Ingestion & Resilience (Weight: 30%)

Build an ingestion layer in Python that pulls data from all API endpoints. Your solution should:

- Handle authentication and automatic token refresh on 401 responses
- Implement retry logic with backoff for 500 and 429 errors
- Validate response payloads and log or quarantine malformed records
- Support paginated extraction across all endpoints
- Be idempotent — running it twice should not produce duplicate data

Save the raw ingested data as your Bronze layer (CSV files in a `bronze/` folder). Add an `_ingested_at` timestamp column and `_source_endpoint` column to each file.

**Data management strategy:** Bronze is append-only — each pipeline run appends new records without overwriting existing data. To prevent duplicates across runs, track which pages or date ranges have already been successfully ingested (e.g. via a simple manifest file or by checking existing records before writing). Re-running the pipeline for the same date range should produce no new rows in Bronze.

---

## Part 2 — Prescribed Star Schema (Weight: 10%)

Implement the following star schema as your Gold layer. The model is fully defined below — your job is to build it correctly through your transformation pipeline.

### Fact Table: `fact_order_items`

**Grain:** one row per item sold in an order

| Column | Type | Source / Logic |
|---|---|---|
| order_item_sk | integer | Surrogate key |
| order_id | string | orders |
| order_item_id | integer | order_items |
| customer_key | integer | FK → dim_customers |
| product_key | integer | FK → dim_products |
| seller_key | integer | FK → dim_sellers |
| order_date | date | orders (purchase_timestamp) |
| order_status | string | orders |
| price | decimal | order_items |
| freight_value | decimal | order_items |
| payment_value | decimal | Total payment for the order (from payments), distributed across items proportionally by item `price`. If an order has 3 items priced at $10, $20, $70, they receive 10%, 20%, 70% of the order's total payment_value respectively. |
| payment_type | string | payments — use the payment type with the highest `payment_value` for the order; if tied, take the first alphabetically |
| payment_installments | integer | payments — use the maximum installment count across payment rows for the order |
| days_to_deliver | integer | Calculated: delivered_customer_date − purchase_timestamp. Null if order not yet delivered. |
| days_delivery_vs_estimate | integer | Calculated: delivered_customer_date − estimated_delivery_date. Positive = late, negative = early. Null if not delivered. |
| is_late_delivery | boolean | True if days_delivery_vs_estimate > 0. Null if not delivered. |

### Dimension: `dim_customers`

**Grain:** one row per unique customer (deduplicated on customer_unique_id)

| Column | Type | Source / Logic |
|---|---|---|
| customer_key | integer | Surrogate key |
| customer_unique_id | string | customers (deduplicate on this, not customer_id) |
| customer_city | string | customers — use the city from the customer's most recent order by purchase_timestamp |
| customer_state | string | customers — use the state from the customer's most recent order |
| customer_zip_code_prefix | string | customers — use from most recent order |
| first_order_date | date | Earliest purchase_timestamp across all orders for this customer |
| total_orders | integer | Count of distinct order_ids |
| total_spend | decimal | Sum of price + freight_value from order_items, across all orders |
| is_repeat_customer | boolean | True if total_orders > 1 |

### Dimension: `dim_products`

**Grain:** one row per product

| Column | Type | Source / Logic |
|---|---|---|
| product_key | integer | Surrogate key |
| product_id | string | products |
| product_category_name | string | products — use "unknown" for null values |
| product_weight_g | decimal | products |
| product_volume_cm3 | decimal | Calculated: product_length_cm × product_height_cm × product_width_cm. Null if any dimension is missing. |
| product_photos_qty | integer | products |
| product_description_length | integer | products |

### Dimension: `dim_sellers`

**Grain:** one row per seller

| Column | Type | Source / Logic |
|---|---|---|
| seller_key | integer | Surrogate key |
| seller_id | string | sellers |
| seller_city | string | sellers |
| seller_state | string | sellers |
| seller_zip_code_prefix | string | sellers |

---

## Part 3 — PySpark Transformation Pipeline (Weight: 30%)

Implement the star schema above through a medallion architecture. Output each layer as CSVs in folders:

```
output/
├── bronze/       # Raw ingested data, one CSV per API endpoint
├── silver/       # Cleaned & typed data, one CSV per source table
└── gold/         # Star schema tables as defined in Part 2
```

### Bronze Layer

Save the raw API responses as-is, one CSV per endpoint. Add `_ingested_at` and `_source_endpoint` metadata columns.

### Silver Layer

Clean and standardize each source table individually:

- Handle nulls (document your strategy per field)
- Cast columns to correct types (dates as dates, decimals as decimals)
- Remove exact duplicate rows
- Flag records with data quality issues in a boolean `_is_valid` column. Examples of invalid records: orders where `delivered_customer_date` is before `purchase_timestamp`, negative prices, order items with no matching order_id.

**Data management strategy:** Silver reflects the current state of each record. Implement upsert logic using the natural key of each table (e.g. `order_id` for orders, `product_id` for products) so that re-processing updates existing records rather than creating duplicates. Bronze's append-only approach feeds into Silver's deduplicated current-state view.

### Gold Layer

Build the four tables exactly as specified in Part 2. Requirements:

- All surrogate keys should be deterministic and reproducible across runs (e.g. using a hash of the natural key rather than a sequential row number)
- All foreign key relationships should be valid — no orphan keys in the fact table
- Include a brief validation step in your code that prints record counts per table and confirms that all `customer_key`, `product_key`, and `seller_key` values in `fact_order_items` exist in their respective dimension tables

---

## Part 4 — SQL Analysis (Weight: 15%)

Write SQL queries against your Gold layer tables. These should be compatible with Spark SQL or T-SQL. For each query, include a brief comment explaining your approach.

### Query 1 — Revenue Trend Analysis with Ranking (Required)

For each product category, calculate monthly revenue (price + freight_value) and rank categories within each month by revenue. Then for the top 5 categories by overall revenue, show their month-over-month revenue growth percentage and a 3-month rolling average of revenue. Only include months where the category had at least 10 transactions.

**Expected output columns:** `product_category_name, year, month, monthly_revenue, monthly_rank, mom_growth_pct, rolling_3m_avg_revenue`

### Query 2 — Seller Performance Scorecard (Stretch)

Build a seller scorecard that ranks sellers on a composite score. For each seller, calculate: late delivery rate (percentage of orders where `is_late_delivery = true`), average `days_delivery_vs_estimate`, total revenue, and order count. Compute a percentile rank for each metric across all sellers — invert the ranking for `late_delivery_rate` and `avg_days_vs_estimate` so that lower values yield higher percentiles. Only include sellers with at least 20 orders. Compute a `composite_score` as a weighted average: on-time delivery percentile (40%), delivery speed percentile (30%), revenue percentile (30%).

**Expected output columns:** `seller_id, seller_state, total_orders, total_revenue, late_delivery_rate, avg_days_vs_estimate, on_time_pctl, speed_pctl, revenue_pctl, composite_score, overall_rank`

> Query 1 is required. Query 2 is a stretch goal — attempt it if time allows, and don't let it crowd out the pipeline work.

---

## Part 5 — README & Technical Decisions (Weight: 15%)

Include a brief README (1–2 pages) in your submission covering:

- Your technical decisions and the reasoning behind them
- How you handled API failures and what your retry/resilience strategy is
- Assumptions you made and any trade-offs you weighed
- What you would change or add for a production deployment on Azure or Microsoft Fabric (scheduling, monitoring, CI/CD, security, cost optimisation)

Be specific rather than generic. A concrete explanation of one real decision — and what you considered before making it — is worth more than a list of buzzwords.

---

## Submission Guidelines

1. Click **"Use this template"** → **"Create a new repository"** at the top of this page
2. Set your new repository to **Public** and give it a name (e.g. `blue-owls-de-assessment`)
3. Do all your work in your repository — commit regularly so we can follow your progress
4. When you are done, submit us the link to your public repository on this form: https://forms.office.com/r/1NvGCj7hQK

Additional requirements:

- **Your entire pipeline must be implemented in Jupyter notebooks** — see [GETTING_STARTED.md](GETTING_STARTED.md) for environment setup
- Include a `requirements.txt` for any packages your code depends on beyond what the notebook image provides
- Notebooks should run end-to-end against the provided API without manual intervention
- Do not commit the raw dataset files — your pipeline should pull from the API

---

## Evaluation Criteria

We assess the following, roughly in order of importance:

| Area | What we look for |
|---|---|
| **Resilience & error handling** | Does the pipeline recover from API failures without crashing or producing corrupt data? |
| **Pipeline correctness** | Does the Gold layer match the prescribed schema with valid data and no orphan keys? |
| **PySpark code quality** | Is the code modular, readable, and clearly structured across notebooks? |
| **SQL correctness** | Is Query 1 correct? Does Query 2 demonstrate analytical depth? |
| **Communication** | Does the README explain real decisions with concrete reasoning? |

**What we don't penalise for:** Not using advanced Spark optimisations (broadcasting, partitioning strategies, caching) — unless you choose to and explain why. Leaving Query 2 incomplete if you've explained your approach.

**What we do penalise for:** Unhandled exceptions that crash the pipeline mid-run. Silent data loss (records dropped with no logging or explanation). A README that lists technologies without explaining decisions.

---

## Challenges & Solutions Encountered

This section documents key challenges encountered during development and the strategies used to resolve them.

### Challenge 1: API Resilience & Authentication Management

**Problem:** The API intentionally returns intermittent 500 errors, 429 rate limits, and 401 authentication failures. Implementing robust retry logic with exponential backoff while managing token refresh proved complex.

**Solution:**
- Implemented a dedicated `TokenManager` class that handles token lifecycle management with automatic refresh on 401 responses
- Built a `retry_with_backoff` decorator supporting configurable:
  - Maximum retries (default: 5)
  - Base delay (0.5s), max delay (30s), and exponential multiplier (2.0)
  - Specific HTTP status codes to retry (500, 429)
- Added request logging and detailed error reporting to diagnose API issues
- Implemented request throttling to respect rate limits proactively before hitting 429 responses

**Key Trade-offs:**
- Chose exponential backoff over linear backoff — exponential respects rate limits better and reduces thundering herd when API recovers
- Set max retries to 5 to balance resilience against pipeline runtime
- Log all retries to `app/logs/` for observability — production would integrate with centralized monitoring

### Challenge 2: Handling Pagination Across Large Datasets

**Problem:** Six data endpoints with unknown total record counts required reliable pagination. Missing the last page or double-fetching pages would corrupt the dataset.

**Solution:**
- Implemented cursor-based pagination tracking in a manifest file (`pipeline_manifest.json`)
- For each endpoint, stored: endpoint name, last successfully processed page, timestamp, and record count
- Built idempotency logic: before fetching page N, verify it hasn't been successfully ingested in the current run
- Used deterministic ordering (by timestamp and ID where available) to ensure consistent pagination

**Bronze Layer Output:**
- Each endpoint produces a separate CSV file with `_ingested_at` and `_source_endpoint` metadata columns
- Append-only bronze avoids overwriting historical data
- New ingestion runs only append records with newer `_ingested_at` timestamps

### Challenge 3: Data Quality Issues in API Responses

**Problem:** The API occasionally returns:
- Null or missing required fields
- Negative prices or freight values
- order items with no matching order_id
- Delivered dates before purchase dates
- Malformed or incomplete JSON records

**Solution:**
- Implemented a comprehensive validation framework in Silver layer with `_is_valid` boolean flag
- Created data quality checks:
  - **Schema validation:** All required columns present and correct types
  - **Business logic validation:** Prices ≥ 0, delivery_date ≥ purchase_date
  - **Referential integrity checks:** order_item references valid order_id, order references valid customer/payment
  - **Completeness checks:** Critical fields (customer_id, order_id, price) not null
- Quarantine invalid records in the `_is_valid = False` rows rather than dropping silently
- Log an ERROR for each invalid record with specific reasons (helps diagnose API/data issues)
- Count invalid records per endpoint and report summary before Gold layer load

**Example handling in Silver:**
```python
orders_clean = orders.withColumn(
    "_is_valid",
    (col("purchase_timestamp").isNotNull()) &
    (col("customer_unique_id").isNotNull()) &
    (col("order_status").isNotNull()) &
    (coalesce(col("delivered_customer_date"), col("purchase_timestamp")) >= col("purchase_timestamp"))
)
```

### Challenge 4: Deterministic Surrogate Key Generation

**Problem:** Surrogate keys must be reproducible across pipeline runs (same input data → same keys). Using row numbers or sequences breaks idempotency.

**Solution:**
- Implemented hash-based surrogate key generation using MD5 hashing of natural keys
- For `dim_customers`: hash `(customer_unique_id)` → deterministic customer_key
- For `dim_products`: hash `(product_id)` → deterministic product_key
- For `dim_sellers`: hash `(seller_id)` → deterministic seller_key
- For `fact_order_items`: hash `(order_id, order_item_id)` → deterministic order_item_sk
- Used Spark's built-in `md5(concat(...))` with 64-bit integer conversion (modulo 2^31-1) to fit within INT range

**Trade-off:** Hash collisions extremely unlikely with MD5 on unique identifiers; if production needed guaranteed uniqueness per run, would use a sequence generator with run_id scope.

### Challenge 5: Complex Payment Attribution to Order Items

**Problem:** Requirements specify payment_value should be distributed across items proportionally by item price. Implementation required:
- Finding the order total payment from the payments table
- Matching each order_item to its order's total payment
- Distributing that payment proportionally across items in the order

**Solution:**
- Window function approach:
  1. Sum all `payment_value` per order_id from payments table
  2. Sum all item `price` per order_id in order_items
  3. For each item: `payment_value = (item.price / order.total_price) * order.total_payment`
  4. Handle division-by-zero: if order total price is 0, distribute payment equally across items
- Used Spark SQL window functions with `OVER (PARTITION BY order_id)` for efficiency

### Challenge 6: Payment Type & Installment Selection Rules

**Problem:** Orders can have multiple payment rows with different payment types and installment counts. Requirements:
- **payment_type:** use the payment type with HIGHEST payment_value; if tied, take first alphabetically
- **payment_installments:** use the MAXIMUM installment count across all payment rows for the order

**Solution:**
- Aggregated at order level post-payment join using:
  - For payment_type: `FIRST(payment_type)` with `ORDER BY payment_value DESC, payment_type ASC`
  - For installments: `MAX(payment_installments)`
- Handled null installments (some payments have NULL) using `COALESCE(..., 0)`

### Challenge 7: Dimensional Data with Slowly Changing Dimensions (Customer Location)

**Problem:** Requirements specify customer dimensions should include "most recent" city/state/zip_code from the customer's most recent order. A customer may have moved or used different addresses across orders.

**Solution:**
- Ranked orders per customer by `purchase_timestamp DESC`
- Selected the first (most recent) row to extract customer location
- This creates a Type 1 slowly changing dimension (always reflect current/latest attribute)
- Identified trade-off: losing historical customer location changes; production would implement Type 2 (versioned dimensions) if needed

### Challenge 8: Query Performance on Large Datasets

**Problem:**
- Query 1 (Revenue Trend Analysis) requires monthly aggregations, ranking, and rolling averages
- Query 2 (Seller Scorecard) requires percentile ranking across sellers

**Solution:**
- Used window functions extensively:
  - `ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY monthly_revenue DESC)` for monthly category ranking
  - `PERCENT_RANK() OVER (ORDER BY metric)` for percentile calculations in seller scorecard
- Filtered to rows meeting minimum thresholds (queries specify ≥10 transactions, ≥20 orders) early in WHERE clause
- Avoided self-joins; used CTEs to calculate aggregates once and reuse

Example pattern:
```sql
WITH monthly_revenue AS (
  SELECT product_category_name, YEAR(...) as year, MONTH(...) as month,
         SUM(price + freight_value) as monthly_revenue
  FROM fact_order_items
  WHERE _is_valid = true
  GROUP BY 1, 2, 3
  HAVING COUNT(DISTINCT order_id) >= 10
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY monthly_revenue DESC) as monthly_rank
FROM monthly_revenue
```

### Challenge 9: Handling Null & Missing Values in Dimensions

**Problem:** Product descriptions, weights, or seller zip codes can be null. Schema requires specific handling.

**Solution:**
- **product_category_name:** Use `COALESCE(product_category_name, 'unknown')` to replace nulls
- **product_weight_g, product_volume_cm3:** Keep as NULL if not provided (represents missing data, not zero)
- **product_volume_cm3:** Calculate as `length × height × width` only if all three dimensions present; otherwise NULL
- **seller_zip, customer_zip:** Keep as NULL if not available (don't fabricate missing codes)

This preserves data lineage and makes null-handling explicit in business logic.

### Challenge 10: Testing & Validation

**Problem:** Ensuring correctness without a reference implementation is difficult. Needed to validate:
- No orphan foreign keys in fact table
- Correct row counts and aggregations
- Payment proportional distribution sums correctly per order

**Solution:**
- Built validation checks as final Gold layer step that:
  1. Counts records per table and compares to expected ranges
  2. Validates all `customer_key`, `product_key`, `seller_key` in `fact_order_items` exist in dimension tables
  3. Spot-checks: sample rows, verify aggregate sums match intermediate calculations
  4. Prints a validation report at pipeline completion
- Created a test notebook that runs sample queries against Gold to verify correctness
- Versioned outputs to track changes and manually inspected a few orders end-to-end through Bronze → Silver → Gold

### Idempotency & Incremental Runs

**Key Design Decision:** The pipeline is designed to be **idempotent** — running it twice against the same date range produces identical results:

1. **Bronze:** Append-only; manifest file tracks which date ranges have been ingested
2. **Silver:** Upsert logic based on natural keys (order_id, customer_id, product_id, seller_id); re-processing overwrites existing records with same natural key
3. **Gold:** Deterministic surrogate key generation; star schema built fresh from current Silver state each run

This enables:
- Fault tolerance — if pipeline fails mid-run, re-running from a checkpoint completes successfully
- Data backfills — re-processing a historical date range updates all downstream layers without duplicates
- Incremental updates — only new/changed records in Silver flow to Gold

---

## Production Readiness (What Would Change)

For deployment to Azure Data Factory, Azure Synapse, or Microsoft Fabric:

1. **Scheduling:** Replace manual notebook execution with Azure Data Factory pipelines (`ScheduledTrigger`, hourly or daily)
2. **Monitoring & Alerting:** Migrate from logging to Application Insights; add alerts for:
   - Pipeline failures (Composite_Trigger → Alert Rule)
   - Data quality issues (_is_valid count > threshold)
   - SLA breaches (end-to-end runtime > expected time)
3. **Security:**
   - Move credentials to Azure Key Vault (instead of env variables)
   - Use Azure Managed Identity for service principal authentication
   - Implement network restrictions (Private Endpoint, VNet rules)
4. **Performance:**
   - Partition Gold tables by order_date for faster queries
   - Use Delta Lake format (on Synapse/Databricks) for ACID guarantees and time-travel
   - Cache intermediate Silver datasets to avoid re-computation
5. **CI/CD:** GitOps workflow with PR validation (lint notebooks, unit test transformations, dry-run against test data)
6. **Cost Optimization:** Resource cleanup (auto-scale Spark clusters), query cost analysis (Synapse Query Store), archive old Bronze data to Blob Storage tiers

---

*Good luck — we look forward to reviewing your work.*
