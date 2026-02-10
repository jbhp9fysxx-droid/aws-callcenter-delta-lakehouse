# AWS Call Center Delta Lakehouse 
End-to-end **AWS Lakehouse analytics project** for a call-center domain using  
**Amazon S3, AWS Glue (Spark), Delta Lake, AWS Glue Data Catalog, and Amazon Athena**.

The project implements a **Silver (Delta) → Gold** architecture with:
- Strong **data quality validation**
- **Delta MERGE (UPSERT)** handling late-arriving and reprocessed data
- Analytics-ready **Gold** metrics
- SQL access via **Athena**

---

## Architecture

### S3 Layout (Lakehouse Layers)

- `s3://<bucket>/source/`  
  Raw CSV input files (initial load + reprocessed / late-arriving files)

- `s3://<bucket>/exception/`  
  Rejected records with `Reject_reason`  
  *(append-only for audit, reprocessing, and governance)*

- `s3://<bucket>/silver_delta/`  
  **Validated Silver layer (Delta Lake)**  
  - Data quality rules applied  
  - Late-arriving data handled via **MERGE (UPSERT)**  
  - Business key: `call_id`  
  - Partitioned by year/month

- `s3://<bucket>/gold/gold_callcenter_analytics/`  
  Analytics-ready **Delta tables**
  - `gold_call_daily_summary/`
  - `gold_agent_performance/`
  - `gold_call_duration_metrics/`


**Processing flow**

1. Source CSV files land in `s3://<bucket>/source/`

2. AWS Glue Spark job applies data quality validations:
   - Valid records → MERGE (UPSERT) into `silver_delta/` (Delta Lake)
   - Invalid records → append into `exception/` with `Reject_reason`

3. Gold analytics job reads from `silver_delta/` and produces Delta tables:
   - `gold_call_daily_summary`
   - `gold_agent_performance`
   - `gold_call_duration_metrics`

4. Glue Crawlers register Delta tables in the Glue Data Catalog

5. Athena queries Gold Delta tables for analytics and reporting

---

## Data Model

### Silver_Delta schema
| column | type |
|---|---|
| call_id | int |
| caller_id | int |
| agent_id | int |
| call_start_time | string (HH:mm:ss) |
| call_end_time | string (HH:mm:ss) |
| call_status | string |
| year | int (partition) |
| month | int (partition) |

### Gold tables

#### 1) `gold_call_daily_summary` (grain: call_date)
Metrics:
- total_calls
- completed_calls
- dropped_calls
- failed_calls
- completion_rate

#### 2) `gold_agent_performance` (grain: agent_id + call_date)
Metrics:
- total_calls
- completed_calls
- dropped_calls
- failed_calls
- completion_rate

#### 3) `gold_call_duration_metrics` (grain: call_date)
Metrics (seconds):
- avg_call_duration_seconds
- min_call_duration_seconds
- max_call_duration_seconds

> Note: Since source contains only time (HH:mm:ss) and no date, `call_date` is derived as the **processing date**.

---

## Key Design Decisions

### Why a single Silver Delta layer?
- Data quality validation is applied **before** data enters Delta
- Only valid records are written to `silver_delta`
- Invalid records are routed to a separate `exception/` path for audit and reprocessing
- Delta Lake enables **MERGE (UPSERT)** to safely handle:
  - Late-arriving data
  - Reprocessed files
  - Idempotent job re-runs

This avoids:
- Duplicate records from append-only pipelines
- Partition overwrite risks
- Maintaining redundant Parquet Silver layers

### Why Delta MERGE?
- Prevents duplicate records when upstream re-sends data
- Updates existing business keys (`call_id`) safely
- Supports repeatable pipelines without data loss

---

## How to Run (Glue Jobs)

> These are AWS Glue Spark jobs. Configure an IAM role with access to S3, Glue, Athena, and CloudWatch.

### 1) Silver Delta Validation + UPSERT Job
- Reads: `source/call_center_raw.csv` (and reprocessed CSVs)
- Applies data quality rules
- Writes:
  - Valid records → `silver_delta/` using Delta MERGE (UPSERT)
  - Invalid records → `exception/` (Parquet, append-only)

### 2) Gold Analytics Job
- Reads: `silver_delta/`
- Produces analytics-ready Delta tables:
  - `gold_call_daily_summary`
  - `gold_agent_performance`
  - `gold_call_duration_metrics`

---

## Querying with Athena

Glue crawlers create tables in the `callcenter_analytics` database:
- `silver_silver_delta`
- `gold_call_daily_summary`
- `gold_agent_performance`
- `gold_call_duration_metrics`

Example:
```sql
SELECT * 
FROM callcenter_analytics.gold_call_daily_summary
ORDER BY call_date DESC
LIMIT 10;
```
---

### Data Quality Rules (Silver Delta)

- `call_id`, `caller_id`, `agent_id` must be **non-null** and **numeric**
- `call_start_time`, `call_end_time` must match the `HH:mm:ss` format
- `call_status` must be one of: `COMPLETED`, `DROPPED`, `FAILED`

**Invalid rows** are routed to the `exception/` directory with a populated `Reject_reason`.

---
### Repository Structure

aws-callcenter-delta-lakehouse/
│
├── glue_jobs/
│   ├── 01_silver_delta_upsert.py      # Validation + Delta MERGE (UPSERT)
│   ├── 02_gold_analytics.py           # Gold aggregations (MERGE)
│
├── sql/
│   └── athena_queries.sq
│
└── README.md

---

### Future Enhancements

- Derive a true `call_date` from source timestamps instead of using the processing date
- Introduce composite business keys and a robust de-duplication strategy beyond `call_id`
- Optimize Delta Lake performance using compaction and retention (VACUUM) policies
- Load Gold-layer Delta tables into **Amazon Redshift** for downstream BI and reporting workloads (optional)

---


