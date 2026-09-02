# Husqvarna Cloud Data & AI Engineer – Data Engineering Assignment

## 1. Project overview

This repository implements an end-to-end data engineering pipeline for messy transactional data.

The pipeline follows the required layered architecture:

```text
Raw CSV
   |
   v
Bronze (raw, append-only)
   |
   v
Silver (cleaned, typed, deduplicated, standardized)
   |
   v
Gold (star schema, Parquet)
   |
   +--> Data Quality Tests
   |
   +--> Analytical Layer
```

The implementation uses Python, PySpark and Parquet. The analytical layer is provided as SQL plus a notebook-backed report.

## 2. Scope

The supplied operational source data contains these CSV files:

- `customers.csv`
- `geolocation.csv`
- `orders.csv`
- `order_items.csv`
- `order_payments.csv`
- `order_reviews.csv`
- `products.csv`
- `sellers.csv`

The taxonomy mapping is maintained separately in `taxonomy/category_translation.csv`.

The supplied taxonomy seed is incomplete. The repository extends it with normalized categories and category families used by the Silver and Gold layers.

## 3. Repository structure

```text
.
├── analysis/
│   ├── analysis.sql
│   └── run_analysis.py
├── data/
│   └── raw/
│       └── source CSV files supplied for the assignment
├── notebooks/
│   └── analysis.ipynb
├── src/
│   ├── __init__.py
│   ├── build_bronze.py
│   ├── build_silver.py
│   ├── build_silver_backup.py
│   ├── build_gold.py
│   ├── data_profile.py
│   ├── integrity_profile.py
│   ├── key_profile.py
│   ├── null_profile.py
│   ├── run_dq_tests.py
│   ├── run_pipeline.py
│   ├── spark_session.py
│   ├── test_idempotency.py
│   ├── validate_gold.py
│   └── validate_silver.py
├── taxonomy/
│   ├── category_profile.py
│   ├── category_translation.csv
│   └── validate_taxonomy.py
├── tests/
├── DEPLOYMENT.md
├── Dockerfile
├── README.md
└── requirements.txt
```

`data/bronze`, `data/silver` and `data/gold` are generated pipeline outputs and are not required as source-controlled submission artifacts.

## 4. Setup

### Prerequisites

- Python 3.10+
- Java 17 for the current PySpark 4.2.0 environment
- PowerShell on Windows, if running locally on Windows

On Windows, the local Spark environment also requires Hadoop Windows utilities. The current development environment uses:

```text
HADOOP_HOME=C:\hadoop
```

Create and activate the virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

## 5. Running the pipeline

From the repository root:

```powershell
python -m src.run_pipeline
```

The pipeline executes:

1. Bronze ingestion
2. Silver transformation
3. Gold transformation
4. Data quality tests

The analytical layer can then be executed with:

```powershell
python -m analysis.run_analysis
```

The notebook report is `notebooks/analysis.ipynb`.

## 6. Data model

The Gold layer implements:

### Dimensions

- `dim_customer`
- `dim_seller`
- `dim_product`
- `dim_date`

### Facts

- `fact_order_item`
- `fact_payment`
- `fact_review`

### Fact grains

| Fact | Grain |
|---|---|
| `fact_order_item` | One row per `(order_id, order_item_id)` |
| `fact_payment` | One row per `(order_id, payment_sequential)` |
| `fact_review` | One row per `(review_id, order_id, product_key)` |

`fact_order_item` is partitioned by purchase year and seller state. `fact_payment` is partitioned by payment year. `fact_review` is partitioned by review year.

Surrogate dimension keys and referential-integrity checks are used.

## 7. Bronze layer

Bronze preserves the source payload with an additional `ingestion_ts` timestamp.

The Bronze implementation is append-only. A repeated ingestion creates a new ingestion batch rather than mutating an existing Bronze record.

Silver reads from Bronze rather than directly from the raw source files.

## 8. Silver layer

Silver performs:

- trimming and standardization of identifiers and text
- timestamp conversion
- numeric typing
- duplicate removal at the appropriate business grain
- category normalization through the extended taxonomy

The taxonomy mapping is `taxonomy/category_translation.csv` with:

```text
source_category
normalized_category
category_family
```

The taxonomy currently contains mappings for all non-null product categories present in the processed dataset.

## 9. Data quality

The pipeline currently executes 16 data-quality checks covering:

- surrogate-key integrity
- fact-to-dimension referential integrity
- fact-grain duplicate detection
- temporal sanity
- non-negative payment values
- taxonomy coverage
- review-score validity
- review-date integrity
- Bronze freshness

The DQ suite is invoked by `src.run_pipeline`, so it is part of the pipeline run.

A failure causes the pipeline to stop.

### Latest verified DQ profiling

The following results were obtained from the latest successful DQ run:

| Test | Rows evaluated | Rows failed | Failure % |
|---|---:|---:|---:|
| `dim_customer.customer_key` is non-null | 99,441 | 0 | 0.00% |
| `dim_seller.seller_key` is non-null | 3,095 | 0 | 0.00% |
| `dim_product.product_key` is non-null | 32,951 | 0 | 0.00% |
| `fact_order_item.customer_key` has no orphans | 112,650 | 0 | 0.00% |
| `fact_order_item.product_key` has no orphans | 112,650 | 0 | 0.00% |
| `fact_order_item` grain is unique | 112,650 | 0 | 0.00% |
| `fact_payment` grain is unique | 103,886 | 0 | 0.00% |
| `delivered_at` is not before `purchased_at` | 110,196 | 0 | 0.00% |
| `payment_value` is non-negative | 103,886 | 0 | 0.00% |
| Non-null product categories are mapped to taxonomy | 32,341 | 0 | 0.00% |
| `fact_review` grain is unique | 102,989 | 0 | 0.00% |
| `fact_review` scores are between 1 and 5 | 102,989 | 0 | 0.00% |
| `fact_review.review_date_key` has no orphans | 102,989 | 0 | 0.00% |
| `fact_review.customer_key` has no orphans | 102,989 | 0 | 0.00% |
| Non-null `fact_review.product_key` has no orphans | 102,230 | 0 | 0.00% |
| Bronze ingestion freshness | N/A | 0 | N/A |

The latest verified run had 16 tests passed and 0 failed. The latest Bronze ingestion age was approximately 0.49 hours, within the 24-hour freshness threshold.

For tests involving optional/null values, only rows for which the rule is applicable are included in the denominator. Bronze freshness is a pipeline-level check, so a row-level failure percentage is not applicable.

## 10. Idempotency

`src/test_idempotency.py` verifies deterministic logical Gold-output hashes after rerunning the same input.

The comparison uses deterministic ordering rather than physical Parquet file ordering.

The currently verified Gold tables are:

- `dim_customer`
- `dim_seller`
- `dim_product`
- `dim_date`
- `fact_order_item`
- `fact_payment`
- `fact_review`

The latest verification produced identical logical hashes for all seven Gold tables.

## 11. Analytical layer

The analytical layer contains five analyses.

### 1. Recent late-delivery performance

Seller × product-category combinations with the highest recent late-delivery rates.

Late delivery:

```text
delivered_at > est_delivery_date
```

The analysis uses a recent 90-day window and a minimum observation count to reduce misleading rates from very small seller-category samples.

### 2. Purchase-to-delivery lag

Average purchase-to-delivery lag by destination state.

### 3. Negative-review MoM movement

Month-over-month movement in negative reviews:

```text
review_score <= 2
```

The analysis exposes both percentage and absolute movement. Small prior-month counts can produce very large percentage changes.

### 4. Order-value anomalies

Order-category value:

```text
item_price + freight_cost
```

is compared with the category p95 baseline.

Multi-category orders are evaluated at order-category contribution level.

### 5. Original analysis

Category-family sales value and unit-volume ranking.

Files:

```text
analysis/analysis.sql
notebooks/analysis.ipynb
```

## 12. Profiling summary

Latest verified Gold row counts:

| Table | Rows |
|---|---:|
| `dim_customer` | 99,441 |
| `dim_seller` | 3,095 |
| `dim_product` | 32,951 |
| `dim_date` | 683 |
| `fact_order_item` | 112,650 |
| `fact_payment` | 103,886 |
| `fact_review` | 102,989 |

### Top findings

- Some recent seller × category combinations have 100% late-delivery rates.
- Average purchase-to-delivery lag varies materially by state, with the highest observed averages above 28 days.
- Some categories show very large MoM increases in negative reviews; small prior-month counts can inflate percentage changes.
- Several order-category values exceed their category p95 baseline by large multiples.
- Home & Living has the highest total value among category families in the current analysis.

## 13. Assumptions and trade-offs

### Source-file inventory

The working dataset contains eight operational source CSVs under `data/raw`. The taxonomy mapping is maintained separately under `taxonomy/`.

### Taxonomy

The supplied taxonomy seed is incomplete, so the repository maintains an extended mapping. The mapping is version-controlled with the repository.

### Reviews and products

Reviews are order-level in the available source structure and do not directly provide a product identifier. Product relationships are populated where an unambiguous product mapping is available. Non-null product foreign keys are checked for referential integrity.

### SCD

The source does not provide a reliable historical-change feed for the dimension attributes used here. An SCD1-style approach is therefore used rather than fabricating historical versions.

### Partitioning

Fact tables are partitioned on fields useful for time/state-oriented access while avoiding unnecessarily fine-grained partitioning.

### Analytical thresholds

The late-delivery analysis applies a minimum observation count to reduce misleading rates from tiny seller-category samples. The negative-review analysis exposes small-denominator effects rather than silently imposing an arbitrary threshold.

### Generated data

Bronze, Silver and Gold Parquet outputs are generated by the pipeline and are not treated as source-controlled submission artifacts.

## 14. Reproducibility

From the repository root:

```powershell
python -m src.run_pipeline
python -m analysis.run_analysis
python -m src.test_idempotency
python -m src.validate_gold
```

The analytical layer is intentionally separate from the core ETL/DQ runner. The Docker workflow chains the pipeline and analytical execution.

## 15. Docker

The repository includes a Dockerfile defining the end-to-end execution flow:

```text
ingest
  -> bronze
  -> silver
  -> gold
  -> tests
  -> analysis
```

Raw input data is supplied to the container at runtime rather than packaging the local generated Bronze/Silver/Gold outputs into the image.

The Docker workflow has been defined in the repository but was not locally executed because Docker is not installed in the current development environment.

See `DEPLOYMENT.md` for the production deployment plan and Docker execution design.

## 16. Assignment deliverables

The repository contains:

- full source code and ingestion scripts
- extended category taxonomy
- README with setup, data model, assumptions and profiling summary
- production deployment plan
- analytical SQL
- analytical notebook
- Dockerfile for end-to-end execution
