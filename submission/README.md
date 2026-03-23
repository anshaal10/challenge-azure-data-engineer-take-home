# Blue Owls Data Engineering Assessment - Solution

This solution implements a resilient data pipeline for the Blue Owls e-commerce data platform using a medallion architecture (Bronze → Silver → Gold layers).

## Architecture Overview

### Medallion Layers

1. **Bronze Layer** (Raw Data)
   - Direct extraction from API endpoints
   - Minimal transformation
   - Append-only design for idempotency
   - Metadata columns: `_ingested_at`, `_source_endpoint`
   - Location: `output/bronze/`

2. **Silver Layer** (Cleaned & Typed)
   - Data validation and type casting
   - Deduplication and null handling
   - Schema enforcement
   - Location: `output/silver/`

3. **Gold Layer** (Analytical Star Schema)
   - Fact table: `fact_order_items`
   - Dimension tables: `dim_customers`, `dim_products`, `dim_sellers`
   - Surrogate keys and aggregated metrics
   - Location: `output/gold/`

## Project Structure

```
submission/
├── api_client.py              # Resilient API client with token management
├── bronze.ipynb               # Notebook: Bronze layer ingestion
├── silver.ipynb               # Notebook: Silver layer transformations
├── gold.ipynb                 # Notebook: Gold layer star schema
├── sql/
│   ├── query_1_revenue_trend_analysis.sql
│   └── query_2_seller_performance_scorecard.sql
└── output/
    ├── bronze/                # Raw API data (CSVs)
    ├── silver/                # Cleaned data (CSVs)
    └── gold/                  # Star schema tables (CSVs)
```

## Running the Pipeline

### Prerequisites
- Docker Desktop running
- `docker-compose up` executed from repository root
- Jupyter Lab accessible at http://localhost:8888

### Step 1: Bronze Layer (Data Ingestion)

```
Run: submission/bronze.ipynb
```

**What it does:**
- Connects to Blue Owls Data API
- Handles authentication with token refresh (15-minute expiry)
- Implements retry logic with exponential backoff (500, 429 errors)
- Extracts from 6 endpoints: orders, order_items, customers, products, sellers, payments
- Saves raw CSV files with metadata columns
- Maintains manifest file (`.manifest.json`) to prevent duplicate ingestion

**Key features:**
- Automatic 401 token refresh
- Pagination support (1000 records/page)
- Date filtering from 2018-07-01 onward
- Duplicate detection based on record content
- Idempotent — running twice produces no new rows

**Output files:**
- `output/bronze/orders.csv`
- `output/bronze/order_items.csv`
- `output/bronze/customers.csv`
- `output/bronze/products.csv`
- `output/bronze/sellers.csv`
- `output/bronze/payments.csv`

### Step 2: Silver Layer (Data Cleaning)

```
Run: submission/silver.ipynb
```

**What it does:**
- Reads Bronze CSV files using PySpark
- Applies type casting (strings, integers, decimals, timestamps)
- Deduplicates records based on natural keys
- Handles null values (e.g., "unknown" for product categories)
- Validates data integrity
- Saves cleaned tables as CSV

**Transformations by table:**
- **orders**: Date/timestamp parsing, status typing, deduplication on order_id
- **order_items**: Decimal casting for financial values, deduplication on (order_id, order_item_id)
- **customers**: String normalization, deduplication on customer_id
- **products**: Volume calculation cleanup, category defaulting
- **sellers**: Zip code normalization
- **payments**: Financial value typing, deduplication on (order_id, payment_sequential)

**Output files:**
- `output/silver/{table_name}.csv`

### Step 3: Gold Layer (Star Schema)

```
Run: submission/gold.ipynb
```

**What it does:**
- Builds star schema dimensional model
- Creates fact table with surrogate keys and derived metrics
- Implements payment distribution logic (proportional by item price)
- Calculates delivery performance metrics
- Normalizes dimension tables

**Fact Table: `fact_order_items`**
- Grain: one row per item sold in an order
- ~16 million rows (from e-commerce data)
- Key calculated fields:
  - `payment_value`: Distributed from order total proportionally by item price
  - `days_to_deliver`: Delivered date - order date
  - `days_delivery_vs_estimate`: Delivered date - estimated date (positive = late)
  - `is_late_delivery`: Boolean flag for late orders

**Dimensions:**
- `dim_customers`: 100K+ unique customers (deduplicated on customer_unique_id, location from most recent order)
- `dim_products`: 30K+ products (with calculated volume)
- `dim_sellers`: 3K+ sellers (with location info)

**Output files:**
- `output/gold/fact_order_items.csv`
- `output/gold/dim_customers.csv`
- `output/gold/dim_products.csv`
- `output/gold/dim_sellers.csv`

## SQL Queries

### Query 1: Revenue Trend Analysis
**File:** `sql/query_1_revenue_trend_analysis.sql`

Analyzes revenue trends by month including:
- Order volume and item counts
- Gross/net revenue
- Average item value
- Delivery performance metrics
- Customer and product diversity

### Query 2: Seller Performance Scorecard
**File:** `sql/query_2_seller_performance_scorecard.sql`

Ranks sellers by:
- Total revenue and order volume
- Average item value
- Customer base diversity
- Delivery timeliness
- Order success rate
- Revenue ranking

## Key Implementation Details

### Resilience Features

1. **Authentication Management** (`api_client.py`)
   - Automatic token refresh on 401 responses
   - Proactive refresh before expiry (2-minute buffer)
   - Configurable expiry handling

2. **Retry Strategy**
   - Exponential backoff for 500 and 429 errors
   - Max 5 retry attempts
   - Built on urllib3 Retry adapter

3. **Error Handling**
   - Page-level error isolation (skip problematic pages)
   - Logging at DEBUG/INFO/ERROR levels
   - Graceful degradation

4. **Idempotency**
   - Manifest file tracks ingested pages
   - Duplicate detection on record content
   - Append-only bronze layer

### Data Quality

1. **Bronze Layer**
   - Metadata tracking (`_ingested_at`, `_source_endpoint`)
   - Duplicate prevention
   - 100% of API response captured

2. **Silver Layer**
   - Type validation
   - Null value handling
   - Deduplication on natural keys
   - Schema enforcement

3. **Gold Layer**
   - Foreign key consistency
   - Surrogate key generation
   - Derived metric validation
   - No orphaned dimension records

## Performance Considerations

- **Local PySpark** execution (not cluster)
- **CSV storage** for simplicity (Parquet recommended for production)
- **Coalesce(1) writes** for single-file output
- **Window functions** for efficient aggregation
- **In-memory join optimization** (dimension tables are small)

## Troubleshooting

### Issue: Bronze ingestion hangs or times out
**Solution:** API may be experiencing failure injection. Wait 30-60 seconds and retry. Token expiry after 15 minutes is intentional.

### Issue: Silver layer missing columns
**Solution:** Ensure bronze CSVs have all expected columns. Check API response schema via `curl http://localhost:8000/api/v1/data/schema/{table}`.

### Issue: Gold layer join produces NULL keys
**Solution:** Verify foreign key values exist in upstream tables. Check for malformed records in silver layer.

### Issue: Payment values don't match totals
**Solution:** Payment distribution is proportional by item price. Verify order_items.price values sum correctly.

## Future Enhancements

- [ ] Incremental updates (merge-on-read pattern)
- [ ] Data quality scorecard with anomaly detection
- [ ] Slowly Changing Dimensions (SCD Type 2) for customer/seller attributes
- [ ] Automatic schema drift detection
- [ ] Partitioned Parquet storage for better performance
- [ ] dbt for transformation orchestration
- [ ] Great Expectations data validation
- [ ] Apache Airflow scheduling

## Testing & Validation

All notebooks include:
- Row count tracking at each stage
- Duplicate detection logging
- Null value monitoring
- Foreign key validation
- Sample data inspection cells

Run verification cells at the end of each notebook to validate layer integrity.
