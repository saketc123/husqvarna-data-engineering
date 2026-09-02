# Deployment Plan

## 1. Production architecture

The recommended production architecture is:

```text
Source CSV / Object Storage
          |
          v
     Orchestrator
          |
          v
Bronze - immutable raw data
          |
          v
Silver - cleaned and standardized data
          |
          v
Gold - star schema / analytical tables
          |
          +------> Data Quality Gates
          |
          v
Analytical / BI Layer
```

The repository currently implements the transformation logic locally with PySpark and Parquet. In production, the same logical stages can run on a managed Spark platform or equivalent cloud data-processing service.

## 2. Orchestration

A production orchestrator such as Airflow, AWS Step Functions, or an equivalent managed workflow service should coordinate:

1. Source availability and schema validation
2. Bronze ingestion
3. Silver transformation
4. Gold transformation
5. Data-quality validation
6. Analytical-layer refresh
7. Monitoring and notification

Each stage should have explicit success/failure state.

Downstream stages must not run when an upstream stage fails.

Transient infrastructure failures can be retried. Data-quality failures should stop the pipeline and generate an actionable alert rather than being silently retried.

## 3. Incremental processing

The current implementation establishes an immutable Bronze layer and deterministic transformations.

A production implementation should add:

- source-level ingestion timestamps or change markers
- per-source watermarks/checkpoints
- batch/run identifiers
- deterministic business keys
- affected-partition detection
- replay metadata
- controlled reprocessing windows

A typical incremental flow is:

```text
New source records
       |
       v
Append to Bronze
       |
       v
Identify affected Silver records/partitions
       |
       v
Transform affected data
       |
       v
Update affected Gold partitions
       |
       v
Run DQ gates
```

The implementation should avoid full-table processing when the source supports reliable incremental identification.

## 4. Late-arriving data

Late-arriving orders, order events, payments, reviews and dimension changes should be handled using event time separately from ingestion time.

Recommended approach:

- preserve original event timestamps
- retain ingestion timestamp in Bronze
- identify late-arriving records during Silver processing
- reprocess the affected time partitions
- run DQ checks over the impacted Gold data
- perform periodic bounded backfills for older data

For dimensions, the production implementation should use a controlled merge/upsert strategy. If reliable historical attribute versions become available, SCD2 can be introduced where business history requires it.

## 5. Idempotency and replay

The pipeline should be safe to rerun for the same input batch.

The current repository supports this through:

- immutable Bronze ingestion
- deterministic Silver transformations
- deterministic surrogate-key generation
- explicit fact grains
- controlled Gold overwrites
- logical Gold-output hash verification

`src/test_idempotency.py` validates that repeated processing of the same logical input produces identical Gold hashes.

In production, orchestration metadata should associate each run with a unique run/batch identifier so that retries do not create duplicate business records.

## 6. Data quality and monitoring

The current pipeline contains 16 DQ checks covering:

- surrogate-key integrity
- referential integrity
- fact-grain uniqueness
- temporal sanity
- payment-value validity
- taxonomy coverage
- review-score validity
- review-date integrity
- Bronze freshness

DQ is executed as part of the pipeline and failures stop downstream processing.

Production monitoring should additionally track:

- source row counts
- Bronze/Silver/Gold row-count deltas
- null rates
- duplicate rates
- referential-integrity failures
- schema changes
- processing duration
- input/output freshness
- late-arriving data volume
- partition sizes
- failed and retried runs

Alerts should be routed to the operational support channel with the affected dataset, run ID, test name and failure metric.

## 7. Schema evolution

Source schemas should be validated before ingestion.

The production pipeline should distinguish between:

- additive, backward-compatible fields
- renamed fields
- removed fields
- type changes
- unexpected columns

Compatible additions can be handled through controlled schema evolution. Breaking changes should fail the ingestion stage and alert the data owner.

The Silver layer should maintain an explicit canonical schema so downstream Gold models are insulated from unnecessary source-level naming changes.

## 8. Secrets and security

Secrets must not be stored in source code or committed to Git.

Production credentials should be supplied through a managed secrets mechanism, such as:

- AWS Secrets Manager
- Azure Key Vault
- an equivalent cloud secret manager

Recommended controls include:

- least-privilege IAM/RBAC
- encryption in transit
- encryption at rest
- managed identities/service accounts where available
- audit logging
- restricted access to raw and curated datasets
- separation of development, staging and production environments

## 9. CI/CD

A production CI/CD pipeline should perform:

```text
Install dependencies
        |
        v
Lint / syntax validation
        |
        v
Unit and DQ tests
        |
        v
Idempotency test
        |
        v
Docker image build
        |
        v
End-to-end container test
        |
        v
Publish versioned artifact
        |
        v
Deploy
```

Pull requests should require automated checks to pass before deployment.

Production artifacts should be versioned so that a known-good pipeline version can be rolled back.

## 10. Docker execution

The repository includes a Dockerfile for the required end-to-end workflow:

```text
ingest
  -> bronze
  -> silver
  -> gold
  -> tests
  -> analysis
```

The image contains the application code and Python/Java runtime dependencies. Raw assignment input is supplied at runtime rather than packaging generated Bronze, Silver and Gold data into the image.

A runtime data mount can provide the expected input/output directory:

```text
/app/data/raw
/app/data/bronze
/app/data/silver
/app/data/gold
```

The Docker workflow is designed to execute the complete pipeline from a clean container.

Docker was not locally executed in the current development environment because Docker is not installed. Therefore, no local Docker execution result is claimed.

## 11. Cost controls

For a workload of approximately 100k orders, production infrastructure should avoid unnecessary distributed compute.

Recommended controls:

- process incrementally rather than rebuilding all historical data
- partition large fact tables by useful time dimensions
- compact small files
- avoid excessively granular partitions
- use columnar Parquet/Delta storage
- prune partitions during analytical queries
- right-size Spark executors
- terminate ephemeral compute after pipeline completion
- apply object-storage lifecycle policies where appropriate
- monitor compute and storage costs by pipeline/job

## 12. Operational recovery

A failed run should retain enough metadata to identify:

- pipeline run ID
- source/input batch
- failed stage
- failed DQ test
- affected dataset
- processing timestamp
- error details

Recovery should normally restart from the failed stage rather than blindly rerunning every stage.

Because Bronze is immutable, historical source payloads remain available for controlled replay.

For a DQ failure:

1. stop downstream publication
2. preserve the failed run's outputs for investigation
3. identify the failing rule and affected records
4. correct the transformation or source issue
5. rerun the affected processing window
6. rerun DQ
7. publish Gold only after the quality gate passes

## 13. Architecture diagram

```text
                    +----------------------+
                    |   Source CSV / Blob  |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |    Orchestrator      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Bronze / Immutable   |
                    | Raw + ingestion_ts   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Silver / Cleaned     |
                    | Typed + Deduplicated |
                    | + Taxonomy           |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Gold / Star Schema   |
                    | Parquet / Delta      |
                    +----------+-----------+
                               |
                 +-------------+-------------+
                 |                           |
                 v                           v
        +----------------+          +------------------+
        | DQ / Monitoring|          | Analytical Layer |
        +----------------+          +------------------+
```

## 14. Current implementation vs production target

| Area | Current repository | Production target |
|---|---|---|
| Compute | Local PySpark | Managed Spark / equivalent |
| Orchestration | Python pipeline runner | Airflow / Step Functions / equivalent |
| Storage | Local Parquet | Cloud object storage + Parquet/Delta |
| Incremental control | Deterministic processing foundation | Watermarks/checkpoints |
| DQ | 16 integrated checks | DQ gates + monitoring/alerting |
| Secrets | Local environment | Managed secret store |
| CI/CD | Repository-ready structure | Automated build/test/deploy |
| Recovery | Stage-level pipeline failure | Run metadata + replay/backfill |
| Monitoring | DQ output | Metrics, dashboards and alerts |
| Docker | Dockerfile | Containerized CI/E2E and deployment |

## 15. Production operating principles

The production implementation should preserve the core engineering properties demonstrated by this repository:

- immutable raw data
- deterministic transformations
- explicit fact grains
- enforced referential integrity
- automated data-quality gates
- reproducible analytical outputs
- controlled incremental processing
- observable pipeline runs
- secure credential management
- versioned deployments
- cost-aware partitioning and compute
