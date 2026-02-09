# aws-callcenter-delta-lakehouse
End-to-end AWS Lakehouse analytics project using S3, Glue, Delta Lake, and Athena, implementing Bronze–Silver–Gold layers with data quality validation, UPSERTs, and analytics-ready Gold metrics for a call-center domain.

# AWS Call Center Delta Lakehouse (S3 + Glue + Delta + Athena)

End-to-end AWS Lakehouse analytics project for a call-center domain using **S3, AWS Glue (Spark), Delta Lake, AWS Glue Data Catalog, and Athena**.

It implements **Bronze → Silver → Silver_Delta → Gold** layers with:
- Data quality validation + rejected record handling
- Delta **MERGE (UPSERT)** for late-arriving / reprocessed data
- Analytics-ready **Gold** aggregates (daily summary, agent performance, duration metrics)
- Querying through **Athena** using Glue Catalog tables

---

## Architecture

**S3 layout**
- `s3://<bucket>/source/`  
  Raw CSV drops (initial + updated files)
- `s3://<bucket>/silver/`  
  Validated Parquet (partitioned by year/month)
- `s3://<bucket>/exception/`  
  Rejected rows with `Reject_reason` (append-only)
- `s3://<bucket>/silver_delta/`  
  Delta table for validated UPSERTed Silver data (partitioned by year/month)
- `s3://<bucket>/gold/gold_callcenter_analytics/`
  - `gold_call_daily_summary/` (Delta)
  - `gold_agent_performance/` (Delta)
  - `gold_call_duration_metrics/` (Delta)

**Processing flow**
1. Source CSV → DQ checks → valid → `silver/` (Parquet) + invalid → `exception/` (Parquet append)
2. Source CSV (updated) → DQ checks → valid → **MERGE into** `silver_delta/` (Delta) + invalid → `exception/`
3. `silver_delta/` → transformations → **MERGE into** Gold Delta tables
4. Crawlers → Glue Catalog → Athena queries

---

## Data Model

### Silver / Silver_Delta schema
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

### Why both `silver/` and `silver_delta/`?
- `silver/` stores validated Parquet outputs (simple, cheap, easy to inspect)
- `silver_delta/` is the **serving Silver** layer used for **UPSERT** and late-arriving data handling
- Keeps validation + exception handling clean, while enabling reliable incremental updates with Delta MERGE

### Why Delta MERGE?
- Prevents duplicates if upstream re-sends the same records
- Supports late-arriving corrections (update existing keys)
- Enables repeatable pipelines without “append chaos”

---

## How to Run (Glue Jobs)

> These are AWS Glue Spark jobs. Configure IAM role with access to S3 + Glue + CloudWatch.

### 1) Silver Validation Job
- Reads: `source/call_center_raw.csv`
- Writes:
  - valid → `silver/` (Parquet, partitioned)
  - invalid → `exception/` (Parquet append)

### 2) Silver Delta UPSERT Job
- Reads: `source/call_center_raw_updated.csv`
- Writes:
  - valid → MERGE into `silver_delta/`
  - invalid → append into `exception/`

### 3) Gold Analytics Job
- Reads: `silver_delta/`
- Writes (MERGE):
  - `gold_call_daily_summary/`
  - `gold_agent_performance/`
  - `gold_call_duration_metrics/`

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

### Data Quality Rules (Silver)

- `call_id`, `caller_id`, `agent_id` must be **non-null** and **numeric**
- `call_start_time`, `call_end_time` must match the `HH:mm:ss` format
- `call_status` must be one of: `COMPLETED`, `DROPPED`, `FAILED`

**Invalid rows** are routed to the `exception/` directory with a populated `Reject_reason`.

---
### Repository Structure

aws-callcenter-delta-lakehouse/
│
├── glue_jobs/
│   ├── 01_silver_validation.py
│   ├── 02_silver_delta_upsert.py
│   └── 03_gold_analytics.py
│
├── sql/
│   └── athena_queries.sql
│
├── docs/
│   └── architecture.md   (optional)
│
└── README.md

---

### Future Enhancements

- Derive a true `call_date` from source timestamps instead of using the processing date
- Introduce composite business keys and a robust de-duplication strategy beyond `call_id`
- Optimize Delta Lake performance using compaction and retention (VACUUM) policies
- Load Gold-layer Delta tables into **Amazon Redshift** for downstream BI and reporting workloads (optional)

---


