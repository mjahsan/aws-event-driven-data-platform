# Silver Layer -- Performance & Reliability Architecture

**Project:** AWS Event-Driven Data Platform\
**Layer:** Silver (Validated → Structured Delta Tables)\
**Last Updated:** 2026-02-24

------------------------------------------------------------------------

## 1. Objective

The Silver layer is responsible for transforming validated JSON event
files into structured, domain-specific Delta tables while ensuring:

-   Idempotent writes
-   Deterministic behavior
-   File-level observability (ETag-based)
-   Controlled shuffle and compute cost
-   Retry safety under orchestration

This document explains the architectural and performance decisions
behind the implementation.

------------------------------------------------------------------------

## 2. Execution Model & Spark Optimization

### 2.1 Lazy Evaluation Awareness

Spark transformations are lazily evaluated. Actions such as:

-   `count()`
-   `collect()`
-   `take()`
-   `rdd.isEmpty()`

trigger full DAG execution.

Early versions included intermediate emptiness checks, causing repeated
scans and unnecessary job execution.

### Final Approach

-   Avoid intermediate actions
-   Accumulate rejection DataFrames
-   Perform a single union and single write
-   Allow MERGE to safely process empty inputs

This minimizes redundant job triggers and improves micro-batch
efficiency.

------------------------------------------------------------------------

## 3. Controlled Caching Strategy

Two DataFrames are reused across validation branches:

-   `df_cont` (flattened container)
-   `df_env` (validated envelope)

These are explicitly persisted:

``` python
df_cont = df_cont.persist()
df_env = df_env.persist()
```

### Rationale

Without caching, Spark recomputes lineage for every branch.\
Selective persistence avoids recomputation while preventing memory
overuse.

------------------------------------------------------------------------

## 4. Shuffle & Partition Strategy

### 4.1 Shuffle Partition Tuning

Default shuffle partitions: 200\
For micro-batch workloads, this is excessive.

``` python
spark.conf.set("spark.sql.shuffle.partitions", "20")
```

This reduces:

-   Task scheduling overhead
-   Small file generation
-   Unnecessary executor fragmentation

------------------------------------------------------------------------

### 4.2 Repartition Before MERGE

Delta MERGE performs a distributed join on `event_id`.

To reduce skew and improve parallelism:

``` python
df.repartition(20, "event_id")
```

This ensures balanced data distribution before MERGE.

Trade-off: Repartition causes shuffle --- but controlled shuffle is
preferable to skewed merge execution.

------------------------------------------------------------------------

## 5. Idempotency & Delta MERGE

The Silver layer must tolerate:

-   Step Function retries
-   Batch replays
-   Duplicate event IDs across files

Implementation:

``` sql
WHEN NOT MATCHED THEN INSERT
```

### Why MERGE?

-   Guarantees idempotent writes
-   Prevents duplicate records
-   Enables safe full-batch reprocessing

MERGE is expensive due to distributed join and Delta transaction cost
--- but correctness outweighs cost in this architecture.

------------------------------------------------------------------------

## 6. Rejection Handling Strategy

Rejected records are:

-   Accumulated
-   Unioned once
-   Written in append mode

``` python
final_rejections.write.mode("append")
```

### Why Not MERGE Rejects?

Rejects are audit data. They are append-only and do not require
deduplication.

Using MERGE for rejects would introduce unnecessary shuffle and
transactional overhead.

------------------------------------------------------------------------

## 7. Failure Semantics

### 7.1 Atomic Delta Guarantees

Delta Lake commits are atomic.

If job fails mid-execution:

-   Partial MERGE does NOT commit
-   Delta transaction log remains consistent
-   Silver tables remain safe

Reject writes may append before failure --- acceptable for audit logs.

------------------------------------------------------------------------

### 7.2 Resume Strategy

The system is intentionally designed for:

**Full-batch retry, not resume-from-middle.**

Reasons:

-   Simplifies orchestration
-   Idempotent MERGE guarantees safety
-   Avoids complex checkpoint tracking

------------------------------------------------------------------------

## 8. File-Level Metrics (ETag-Based Observability)

Metrics are computed per ETag:

-   total_events
-   rejected_events
-   valid_events
-   file_status

File status rules:

  Condition      Status
  -------------- -----------------
  valid = 0      FAILED
  rejected = 0   SUCCESS
  otherwise      PARTIAL_SUCCESS

Batch status returned to Step Function: `"COMPLETED"` unless Spark
crashes.

This design separates orchestration status from file-level health.

------------------------------------------------------------------------

## 9. Table Partitioning

Silver tables are partitioned by:

``` sql
PARTITIONED BY (event_date)
```

Benefits:

-   Partition pruning
-   Faster time-based aggregation
-   Reduced scan footprint for Gold layer

------------------------------------------------------------------------

## 10. Z-ORDER (Future Optimization)

For large Silver tables:

``` sql
OPTIMIZE table_name ZORDER BY (event_id);
```

Improves:

-   Data skipping
-   MERGE performance
-   Query efficiency on selective keys

Not required for small datasets, critical at scale.

------------------------------------------------------------------------

## 11. Gold Layer Forward Strategy

To avoid full-table scans in Gold:

-   Track last processed event timestamp externally
-   Process only new partitions
-   Perform incremental aggregation

This prevents scanning entire Silver tables as volume grows.

------------------------------------------------------------------------

## 12. Design Principles

The Silver layer prioritizes:

-   Determinism
-   Idempotency
-   Controlled shuffle
-   Minimal redundant execution
-   File-level observability
-   Retry safety over micro-optimization

This is not just a transformation job ---\
it is a distributed system component designed for production
reliability.

------------------------------------------------------------------------

