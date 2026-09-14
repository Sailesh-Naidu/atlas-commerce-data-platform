# ADR-004: Customer Snapshot–CDC Reconciliation Architecture

**Status:** Accepted / Implemented  
**Date:** 2026-09-14  
**Domain:** Customer  
**Subdomains:** `customers`, `customer_addresses`, `customer_consents`  
**Platform:** Atlas Commerce Data Platform  
**Primary technologies:** Apache Spark, Delta Lake, PostgreSQL, Debezium, Kafka  

---

## 1. Decision Summary

Atlas reconciles an **authoritative point-in-time source snapshot** against an independently maintained **CDC-derived expected state**.

The reconciliation system is designed to answer:

> “At snapshot time `T`, does the state reconstructed from accepted CDC events match the authoritative source snapshot?”

The implemented design uses:

1. An authoritative snapshot exported from the source system.
2. Silver CDC history as the event source for expected-state construction.
3. A persistent materialized `expected_state` Delta table.
4. Incremental expected-state advancement using only CDC events between the previous and current reconciliation cutoffs.
5. Bucket-level checksum comparison to avoid unnecessary row-level comparison.
6. Row-level comparison only for mismatching buckets.
7. Exception-only persistence.
8. Append-only run metadata for auditing.
9. UTC timestamps for all reconciliation cutoffs.

The reconciliation process **detects and records mismatches**. It does **not automatically repair** the source, Silver CDC history, or expected state from snapshot differences.

---

# 2. Why Reconciliation Exists

CDC is fast and suitable for keeping Silver state current, but a CDC pipeline can still experience problems such as:

- missed events,
- duplicated events,
- out-of-order events,
- delayed events,
- incorrect ordering decisions,
- downstream bugs,
- processing failures,
- source/CDC divergence.

Therefore Atlas uses two independent views of the same business state:

```text
Source system
   │
   ├──────────────► Authoritative snapshot at time T
   │
   └► Debezium ► Kafka ► Bronze ► Silver CDC History
                                  │
                                  ▼
                           Expected State at T
                                  │
                                  ▼
                         Reconciliation
```

The snapshot acts as the independent control dataset.

The expected state represents what Atlas believes the source should contain based on accepted CDC history.

If the two disagree, reconciliation records the discrepancy for investigation.

---

# 3. Scope

This ADR applies to Customer-domain reconciliation for:

```text
customer/customers
customer/customer_addresses
customer/customer_consents
```

The same reconciliation mechanics are reused across these entities, while entity-specific code owns:

- snapshot schema,
- entity key,
- business validation,
- checksum normalization,
- source paths,
- comparison columns.

Entity keys are:

| Entity | Reconciliation key |
|---|---|
| `customers` | `customer_id` |
| `customer_addresses` | `address_id` |
| `customer_consents` | `consent_id` |

---

# 4. Important Terminology

## 4.1 Authoritative Snapshot

A snapshot is a complete representation of source-system state at a known logical cutoff.

Example:

```text
snapshot_as_of = 2026-09-14 09:55:00 UTC
```

This means:

> “The snapshot represents what the source system contained at 09:55 UTC.”

The CSV file itself is not authoritative merely because it is a CSV. It is authoritative because Atlas treats it as an independent export of the source state at a defined point in time.

---

## 4.2 CDC History

Silver CDC history is an append-oriented record of accepted source changes.

It retains CDC events such as:

```text
c = create
u = update
r = snapshot/read
d = delete
```

Relevant ordering/control fields include:

```text
source_timestamp
source_lsn
kafka_partition
kafka_offset
cdc_operation
```

For reconciliation:

- `source_timestamp` decides whether an event belongs before a cutoff.
- `source_lsn` is the primary source ordering field.
- `kafka_offset` is the tie-breaker where required.
- `cdc_timestamp` is useful for latency/observability, not reconciliation membership.

---

## 4.3 Expected State

`expected_state` is Atlas's materialized CDC-derived view of what the source should contain at the most recently processed reconciliation cutoff.

It is **not** copied from the authoritative snapshot.

It is built independently from CDC history.

That independence is essential. If expected state were overwritten from the snapshot, reconciliation would stop being a meaningful control.

---

## 4.4 Reconciliation Cutoff

The cutoff is `snapshot_as_of`.

For a run at time `T`:

```text
expected state at T
        vs
authoritative snapshot at T
```

must refer to the same logical point in time.

---

# 5. Time-Zone Contract

All Atlas reconciliation cutoffs are interpreted as **UTC**.

This applies to:

```text
snapshot_as_of
state_as_of
previous_state_as_of
source_timestamp
```

Spark is configured with:

```text
spark.sql.session.timeZone = UTC
```

Therefore this:

```python
snapshot_as_of = "2026-09-14 09:55:00"
```

means:

```text
2026-09-14 09:55:00 UTC
```

It must **not** be a local IST wall-clock value passed without conversion.

For example:

```text
15:25 IST = 09:55 UTC
```

The correct Atlas cutoff is therefore:

```text
2026-09-14 09:55:00
```

not:

```text
2026-09-14 15:25:00
```

Mixing local time with UTC can silently skip valid CDC events because incremental processing uses:

```text
previous_state_as_of < source_timestamp <= current_state_as_of
```

---

# 6. Physical Reconciliation Storage

Each entity owns a separate reconciliation area.

Example:

```text
data/lakehouse/ops/reconciliation/
└── customer/
    ├── customers/
    │   ├── expected_state/
    │   ├── expected_state_metadata/
    │   ├── run_summary/
    │   └── exception_detail/
    │
    ├── customer_addresses/
    │   ├── expected_state/
    │   ├── expected_state_metadata/
    │   ├── run_summary/
    │   └── exception_detail/
    │
    └── customer_consents/
        ├── expected_state/
        ├── expected_state_metadata/
        ├── run_summary/
        └── exception_detail/
```

These are operational reconciliation datasets and therefore live under:

```text
ops/reconciliation
```

rather than under Silver.

---

# 7. What Each Delta Dataset Represents

## 7.1 `expected_state`

### Purpose

Stores the **current materialized CDC-derived state** for the entity.

Example:

```text
ops/reconciliation/customer/customers/expected_state
```

It contains one current row per live entity key.

For customers:

```text
customer_id = one current customer row
```

For addresses:

```text
address_id = one current address row
```

For consents:

```text
consent_id = one current consent row
```

### Important behavior

The table is not a daily archive.

It represents only the latest materialized expected state.

On the first reconciliation run:

```text
full CDC history up to T
        ↓
latest valid event per key
        ↓
remove deletes
        ↓
expected_state
```

On later runs:

```text
previous expected_state
        +
new CDC events since previous cutoff
        ↓
Delta MERGE
        ↓
new expected_state
```

### Merge semantics

For incoming latest CDC events:

```text
c / u / r
    → insert or update

d
    → delete from expected_state
```

Conceptually:

```text
MATCHED + c/u/r     → UPDATE
MATCHED + d         → DELETE
NOT MATCHED + c/u/r → INSERT
NOT MATCHED + d     → NO ROW CREATED
```

### `state_updated_as_of`

Rows contain a field such as:

```text
state_updated_as_of
```

This indicates the reconciliation cutoff that last changed that expected-state row.

Unchanged records retain their previous value.

---

# 7.2 `expected_state_metadata`

### Purpose

Append-only history describing **how expected state advanced over time**.

It answers:

> “What cutoff has expected state processed through?”

This is separate from reconciliation outcome.

### Typical columns

```text
entity_name
previous_state_as_of
state_as_of
records_inserted
records_updated
records_deleted
processed_cdc_rows
state_row_count
reconciliation_run_id
processed_at
```

### Meaning of each field

#### `entity_name`

Entity being processed.

Examples:

```text
customers
customer_addresses
customer_consents
```

#### `previous_state_as_of`

Previous successfully advanced expected-state cutoff.

Bootstrap run:

```text
NULL
```

Incremental run:

```text
T1
```

#### `state_as_of`

Current cutoff through which expected state has been advanced.

Example:

```text
2026-09-14 09:55:00 UTC
```

#### `records_inserted`

Rows inserted into expected state during the current advancement.

#### `records_updated`

Rows updated in expected state.

#### `records_deleted`

Rows deleted from expected state because their latest incremental event was `d`.

#### `processed_cdc_rows`

Number of raw CDC events inside the incremental interval:

```text
(previous_state_as_of, state_as_of]
```

This is intentionally different from the number of final merge operations.

Example:

```text
customer 2:
    update A
    update B

customer 3:
    update

customer 4:
    delete

customer 9:
    create
```

Raw interval:

```text
processed_cdc_rows = 5
```

Latest event per key:

```text
customer 2 → update B
customer 3 → update
customer 4 → delete
customer 9 → create
```

Merge operations:

```text
4
```

Therefore:

```text
processed_cdc_rows = 5
records_inserted + records_updated + records_deleted = 4
```

That is expected.

#### `state_row_count`

Number of live rows in expected state after the merge.

#### `reconciliation_run_id`

UUID connecting the expected-state advancement to the reconciliation execution.

#### `processed_at`

Actual job execution timestamp.

This is different from `state_as_of`.

```text
state_as_of = business/data cutoff
processed_at = when Spark executed the work
```

---

# 7.3 `run_summary`

### Purpose

Append-only audit table describing the **result of a reconciliation run**.

It answers:

> “Did expected state agree with the authoritative snapshot?”

It does **not** determine how far expected state has progressed.

Expected-state progression is tracked by:

```text
expected_state_metadata
```

### Current metrics

```text
snapshot_row_count
cdc_row_count
exception_count
missing_in_cdc_count
missing_in_snapshot_count
checksum_mismatch_count
matched_row_count
overall_status
reconciliation_run_id
entity_name
snapshot_as_of
reconciled_at
```

These metrics are described in detail later in this ADR.

---

# 7.4 `exception_detail`

### Purpose

Stores only rows where reconciliation found a discrepancy.

Atlas intentionally does **not** persist every matched row.

This avoids turning reconciliation into another full copy of both datasets.

Typical exception columns include:

```text
entity key
snapshot_row_checksum
cdc_row_checksum
snapshot_present
cdc_present
reconciliation_status
snapshot_<business columns>
cdc_<business columns>
mismatch_columns
reconciliation_run_id
snapshot_as_of
reconciled_at
entity_name
```

Possible statuses:

```text
MISSING_IN_CDC
MISSING_IN_SNAPSHOT
CHECKSUM_MISMATCH
```

A successful run can therefore have:

```text
0 rows
```

in `exception_detail`.

That is expected.

---

# 8. Delta Lake `_delta_log` vs Atlas Business Logs

Every Delta table above contains an internal directory:

```text
_delta_log/
```

Example:

```text
expected_state/_delta_log/
run_summary/_delta_log/
```

This internal Delta transaction log is **not the same thing as Atlas reconciliation metadata**.

The internal `_delta_log` records Delta Lake commits such as:

```text
files added
files removed
schema metadata
transaction versions
MERGE commits
WRITE commits
```

Atlas business-level meaning is stored in tables such as:

```text
expected_state_metadata
run_summary
exception_detail
```

Therefore:

```text
Delta _delta_log
    = storage/transaction history

Atlas expected_state_metadata
    = state advancement history

Atlas run_summary
    = reconciliation outcome history

Atlas exception_detail
    = business discrepancy history
```

---

# 9. End-to-End Reconciliation Flow

The complete run can be divided into six phases.

```text
1. Load and validate snapshot
2. Build/advance expected state
3. Calculate normalized row checksums
4. Compare buckets
5. Drill down mismatching buckets
6. Persist metrics and exceptions
```

---

# 10. Phase 1 — Snapshot Loading and Validation

The authoritative snapshot is loaded using an explicit entity-specific schema.

Examples:

```text
customers
customer_addresses
customer_consents
```

Snapshot metadata is then attached.

Conceptually:

```text
CSV snapshot
    ↓
explicit schema
    ↓
snapshot metadata
    ↓
entity-specific DQ
    ↓
valid snapshot rows
```

Invalid or ambiguous records are excluded from reconciliation-ready data.

Examples of entity-specific checks:

### Customer

```text
customer_id required
first_name required
last_name required
email or phone required
date_of_birth not in future
valid status
valid segment
```

### Address

```text
address_id required
customer_id required
valid address_type
address_line_1 required
city required
postal_code required
country required
is_primary required
```

### Consent

```text
consent_id required
customer_id required
valid consent_type
granted required
```

PostgreSQL booleans exported as:

```text
t
f
```

are read as strings and explicitly normalized to:

```text
true
false
```

before DQ.

---

# 11. Phase 2 — Expected-State Construction

There are two execution modes:

```text
bootstrap
incremental
```

---

# 12. Bootstrap Expected-State Flow

Bootstrap happens when:

```python
DeltaTable.isDeltaTable(spark, expected_state_path) == False
```

Flow:

```text
Silver CDC history
      ↓
filter source_timestamp <= snapshot_as_of
      ↓
partition by entity key
      ↓
order by source_lsn DESC, kafka_offset DESC
      ↓
take latest event per key
      ↓
remove latest deletes
      ↓
write expected_state
      ↓
append bootstrap expected_state_metadata
```

Ordering rule:

```text
source_lsn DESC
kafka_offset DESC
```

The latest event determines current expected entity state.

If latest event is:

```text
d
```

the entity does not appear in expected state.

---

# 13. Incremental Expected-State Flow

Once expected state exists, the system does **not rebuild from all historical CDC**.

Instead:

```text
expected_state_metadata
        ↓
latest state_as_of = T1
```

Then:

```text
CDC history
    ↓
filter:
    source_timestamp > T1
    AND
    source_timestamp <= T2
```

where:

```text
T2 = current snapshot_as_of
```

Therefore the interval is:

```text
(T1, T2]
```

The lower bound is exclusive.

The upper bound is inclusive.

---

## 13.1 Why `(T1, T2]`?

Events at exactly `T1` have already been included in the previous expected state.

Reprocessing them would be unnecessary.

Events at exactly `T2` belong to the current snapshot cutoff and must be included.

---

# 14. Latest Incremental Event Per Entity

The interval may contain multiple changes to the same entity.

Example:

```text
customer 2:
    LSN 100 → ACTIVE
    LSN 120 → SUSPENDED
```

Expected state only needs the latest event:

```text
LSN 120
```

Spark therefore applies:

```text
PARTITION BY entity_key
ORDER BY source_lsn DESC, kafka_offset DESC
ROW_NUMBER = 1
```

This produces one final state-changing event per entity for the merge.

---

# 15. Delta MERGE Into Expected State

The latest event per affected entity is merged into expected state.

Conceptually:

```sql
WHEN MATCHED AND operation IN ('c','u','r')
    UPDATE

WHEN MATCHED AND operation = 'd'
    DELETE

WHEN NOT MATCHED AND operation IN ('c','u','r')
    INSERT
```

A delete for a key not already present produces no expected-state row.

After MERGE:

```text
expected_state = current CDC-derived state as of T2
```

---

# 16. Expected-State Metadata Is Written After Advancement

After expected-state mutation, Atlas appends one metadata row.

Example:

```text
previous_state_as_of = 2026-09-14 09:50:00
state_as_of          = 2026-09-14 09:55:00
records_inserted     = 1
records_updated      = 2
records_deleted      = 1
processed_cdc_rows   = 5
state_row_count      = 13
```

This means:

> Expected state successfully advanced from 09:50 through 09:55.

It does **not** mean reconciliation was successful.

The snapshot comparison occurs separately.

---

# 17. Phase 3 — Row Normalization and Checksums

Direct raw comparison is avoided because semantically equivalent values may differ physically.

Examples:

```text
"ACTIVE"
" active "
```

or:

```text
"USER@MAIL.COM"
"user@mail.com"
```

Entity-specific normalization creates stable comparison values.

Typical normalization:

```text
trim strings
lowercase email
uppercase status / segment / enum values
stable timestamp formatting
explicit null sentinel
boolean cast to string
```

NULL is represented explicitly as:

```text
__NULL__
```

This prevents:

```text
NULL
```

from accidentally becoming equivalent to:

```text
""
```

---

# 18. Row Checksum

Normalized fields are concatenated in deterministic order and hashed using SHA-256.

Conceptually:

```text
normalized_col_1
|| normalized_col_2
|| normalized_col_3
...
        ↓
SHA-256
        ↓
row_checksum
```

If snapshot and expected-state row checksums match:

```text
business state matches
```

If they differ:

```text
row requires detailed comparison
```

---

# 19. Why `created_at` Is Usually Excluded

For current Customer reconciliation, `updated_at` participates in the checksum while `created_at` is generally excluded from business comparison.

The purpose is to compare current authoritative business state rather than every historical metadata field.

Entity-specific implementations remain responsible for choosing reconciliation columns.

---

# 20. Phase 4 — Bucket-Level Comparison

Comparing every row with a full outer join can be expensive on large datasets.

Atlas therefore assigns each entity key to a deterministic bucket.

Conceptually:

```python
bucket_id = pmod(hash(entity_key), bucket_count) + 1
```

Both snapshot and expected-state rows for the same key always land in the same bucket.

---

# 21. Bucket Checksum

Within each bucket:

```text
row checksums
      ↓
collect
      ↓
sort deterministically
      ↓
concatenate
      ↓
SHA-256
```

Bucket metrics include:

```text
bucket_row_count
bucket_checksum
```

Sorting is mandatory because distributed `collect_list` ordering is not deterministic.

---

# 22. Bucket Statuses

Snapshot buckets and expected-state buckets are full-outer-joined.

Possible bucket statuses:

```text
MISSING_IN_SNAPSHOT
MISSING_IN_CDC
COUNT_MISMATCH
CHECKSUM_MISMATCH
MATCH
```

Priority is:

```text
missing side
    ↓
count mismatch
    ↓
checksum mismatch
    ↓
match
```

---

# 23. Why Bucket Comparison Exists

If a bucket has:

```text
same count
same checksum
```

Atlas considers the entire bucket matched.

No row-level full outer join is required for that bucket.

Only mismatching buckets proceed to row-level comparison.

This is the main reconciliation comparison optimization.

It does **not** eliminate the cost of expected-state construction. That is why incremental expected state was added separately.

---

# 24. Phase 5 — Row-Level Drill-Down

Rows belonging to mismatching buckets are selected using semi-join semantics.

Then snapshot and expected-state rows are full-outer-joined by entity key.

Presence markers are attached before the join:

```text
snapshot_present
cdc_present
```

These are required because business fields themselves can legitimately be NULL.

---

# 25. Row-Level Reconciliation Status

The row status logic is:

```text
snapshot absent + CDC present
    → MISSING_IN_SNAPSHOT

snapshot present + CDC absent
    → MISSING_IN_CDC

both present + checksum differs
    → CHECKSUM_MISMATCH

otherwise
    → MATCH
```

Even inside a mismatching bucket, individual rows can still be `MATCH`.

Only exception statuses are persisted.

---

# 26. Field-Level Mismatch Diagnostics

For rows with:

```text
CHECKSUM_MISMATCH
```

Atlas compares selected snapshot and CDC fields using null-safe equality.

Spark's normal equality:

```text
NULL = NULL
```

does not return `true`.

Therefore reconciliation uses null-safe semantics:

```python
eqNullSafe()
```

The result is an array such as:

```text
[
  "GRANTED_MISMATCH",
  "UPDATED_AT_MISMATCH"
]
```

or:

```text
[
  "STATUS_MISMATCH"
]
```

This becomes:

```text
mismatch_columns
```

---

# 27. Example Exception Interpretation

Example consent result:

```text
consent_id = 5

snapshot_granted = false
cdc_granted      = true

reconciliation_status = CHECKSUM_MISMATCH
mismatch_columns       = [GRANTED_MISMATCH, UPDATED_AT_MISMATCH]
```

This means:

> The authoritative source snapshot and CDC-derived expected state both contain consent `5`, but their business state differs.

Example:

```text
consent_id = 12

snapshot_present = NULL
cdc_present      = true

reconciliation_status = MISSING_IN_SNAPSHOT
```

Meaning:

> CDC-derived expected state still believes consent `12` exists, but the authoritative snapshot does not.

Example:

```text
consent_id = 15

snapshot_present = true
cdc_present      = NULL

reconciliation_status = MISSING_IN_CDC
```

Meaning:

> The source snapshot contains consent `15`, but the CDC-derived expected state does not.

---

# 28. Phase 6 — Reconciliation Metrics

After exception generation, Atlas calculates run-level metrics.

---

# 29. `snapshot_row_count`

Definition:

```text
number of valid authoritative snapshot rows
```

This count is taken after snapshot validation.

Example:

```text
snapshot_row_count = 13
```

---

# 30. `cdc_row_count`

Definition:

```text
number of rows in expected_state used for this reconciliation
```

Despite the historical name `cdc_row_count`, this is the row count of the reconstructed/materialized CDC-derived state.

Example:

```text
cdc_row_count = 13
```

---

# 31. `exception_count`

Definition:

```text
MISSING_IN_CDC
+ MISSING_IN_SNAPSHOT
+ CHECKSUM_MISMATCH
```

Equivalent to the number of rows persisted to `exception_detail` for the run.

Example:

```text
exception_count = 4
```

---

# 32. `missing_in_cdc_count`

Number of keys that exist in the authoritative snapshot but not expected state.

```text
snapshot present
CDC absent
```

Possible causes include:

- missing CDC event,
- incorrect CDC rejection,
- state reconstruction error,
- CDC lag,
- cutoff error.

---

# 33. `missing_in_snapshot_count`

Number of keys present in expected state but absent from the authoritative snapshot.

```text
CDC present
snapshot absent
```

Possible causes include:

- missed delete,
- stale expected state,
- incorrect cutoff,
- snapshot extraction issue.

---

# 34. `checksum_mismatch_count`

Number of keys present on both sides whose normalized row checksums differ.

Possible causes include:

- missing update,
- stale update,
- source/CDC divergence,
- normalization issue,
- timestamp mismatch,
- processing bug.

---

# 35. `matched_row_count`

Atlas computes matched rows from the authoritative snapshot population.

Current logic:

```text
matched_row_count
    =
snapshot_row_count
- missing_in_cdc_count
- checksum_mismatch_count
```

`missing_in_snapshot_count` is deliberately **not subtracted**.

Why?

A row that exists only in expected state was never part of the authoritative snapshot population.

Example:

```text
snapshot_row_count        = 13
missing_in_cdc_count      = 1
checksum_mismatch_count   = 2
missing_in_snapshot_count = 1
```

Then:

```text
matched_row_count
= 13 - 1 - 2
= 10
```

This matches the implemented result.

---

# 36. Why Matched Rows Are Not Counted From Row-Level Output

Matched buckets never go through row-level drill-down.

Therefore row-level comparison does not contain all matched records.

Using row-level `MATCH` count would undercount success.

The formula based on snapshot totals is therefore required.

---

# 37. `overall_status`

Current semantics:

```text
exception_count = 0
    → SUCCESS

exception_count > 0
    → COMPLETED_WITH_EXCEPTIONS
```

A third conceptual state exists:

```text
FAILED
```

but that represents process execution failure and belongs to job/orchestration handling rather than data mismatch classification.

---

# 38. `reconciliation_run_id`

Generated once per reconciliation execution:

```python
uuid.uuid4()
```

The same run ID connects:

```text
expected_state_metadata
run_summary
exception_detail
```

This makes a run traceable across operational tables.

---

# 39. `snapshot_as_of`

Logical source-state cutoff being reconciled.

It is a business/runtime parameter.

It must never silently default to job execution time.

---

# 40. `reconciled_at`

Actual time at which reconciliation results were created.

This is operational execution metadata.

It is not the business cutoff.

---

# 41. Successful Run Example

Example:

```text
snapshot_row_count        = 13
cdc_row_count             = 13
exception_count           = 0
missing_in_cdc_count      = 0
missing_in_snapshot_count = 0
checksum_mismatch_count   = 0
matched_row_count         = 13
overall_status            = SUCCESS
```

Meaning:

> The authoritative snapshot and the CDC-derived expected state agree completely at the requested cutoff.

`exception_detail` contains no rows for that run.

---

# 42. Exception Run Example

Observed Customer Consent example:

```text
snapshot_row_count        = 13
cdc_row_count             = 13
exception_count           = 4
missing_in_cdc_count      = 1
missing_in_snapshot_count = 1
checksum_mismatch_count   = 2
matched_row_count         = 10
overall_status            = COMPLETED_WITH_EXCEPTIONS
```

The four exceptions were:

```text
2 checksum mismatches
1 missing in CDC
1 missing in snapshot
```

This demonstrates that equal total row counts do **not** guarantee equal state.

---

# 43. Bootstrap Example

Suppose the first Customer reconciliation cutoff is:

```text
T1 = 2026-09-14 00:00:00 UTC
```

Flow:

```text
all CDC history where source_timestamp <= T1
        ↓
latest event per customer
        ↓
remove deletes
        ↓
expected_state
        ↓
expected_state_metadata:
    previous_state_as_of = NULL
    state_as_of          = T1
```

Then:

```text
snapshot T1
    vs
expected_state T1
```

is reconciled.

---

# 44. Incremental Example

Previous metadata:

```text
state_as_of = 2026-09-14 09:50:00
```

Current snapshot:

```text
snapshot_as_of = 2026-09-14 09:55:00
```

CDC interval:

```text
09:50 < source_timestamp <= 09:55
```

Suppose interval contains five events:

```text
customer 2 update
customer 3 update
customer 4 delete
customer 2 later update
customer 9 create
```

Then:

```text
processed_cdc_rows = 5
```

Latest per key:

```text
customer 2 → later update
customer 3 → update
customer 4 → delete
customer 9 → create
```

Delta MERGE metrics:

```text
records_updated  = 2
records_deleted  = 1
records_inserted = 1
```

Expected-state metadata:

```text
previous_state_as_of = 09:50
state_as_of          = 09:55
processed_cdc_rows   = 5
records_updated      = 2
records_deleted      = 1
records_inserted     = 1
```

---

# 45. Separation of State Progress and Reconciliation Result

This distinction is critical.

## Expected-state metadata answers:

```text
How far has the CDC-derived state been advanced?
```

## Run summary answers:

```text
Did that expected state agree with the snapshot?
```

These are not the same question.

Therefore the next incremental cutoff must come from:

```text
expected_state_metadata.state_as_of
```

not:

```text
run_summary.snapshot_as_of
```

---

# 46. Why Expected State Can Advance Even With Exceptions

Suppose:

```text
expected_state successfully advances to T2
```

but comparison against the T2 snapshot finds mismatches.

Atlas still records:

```text
state_as_of = T2
```

because expected-state processing itself completed.

The snapshot discrepancy is recorded separately.

This prevents reconciliation mismatches from incorrectly causing CDC state to replay old intervals forever.

---

# 47. Append-Only Audit Semantics

`run_summary` and `exception_detail` are append-only.

A rerun for the same snapshot cutoff receives a new:

```text
reconciliation_run_id
```

Therefore multiple rows can exist for the same:

```text
entity_name
snapshot_as_of
```

This is expected.

It preserves operational history.

The latest completed run can be treated as the current operational result when needed.

---

# 48. Why Reconciliation Does Not Auto-Repair

Current Atlas policy:

```text
detect
classify
persist
investigate
```

not:

```text
detect
silently overwrite
```

An authoritative snapshot mismatch can indicate many different problems.

Automatically changing expected state from snapshot data would:

- hide CDC defects,
- destroy independent validation,
- make root-cause analysis harder,
- risk propagating source extraction errors.

Therefore mismatches are persisted only.

---

# 49. Current Scalability Design

The original design rebuilt state from all historical CDC for every run.

That becomes expensive because it requires:

```text
full CDC history scan
+
window by entity key
+
sort by source ordering
```

for every reconciliation.

The implemented scale improvement is:

```text
yesterday's expected state
        +
CDC since yesterday's cutoff
        ↓
today's expected state
```

This converts expected-state advancement from historical reconstruction into incremental processing.

---

# 50. What Bucket Checksums Solve — and Do Not Solve

Bucket checksums reduce the cost of:

```text
snapshot vs expected-state detailed row comparison
```

because matching buckets never reach row-level join.

They do **not** solve:

```text
full historical expected-state reconstruction
```

That is why Atlas uses both:

```text
incremental expected state
+
bucket comparison
```

---

# 51. Current Known Limitations

## 51.1 Two Delta writes are not atomic across tables

Expected-state update and expected-state-metadata append are separate Delta transactions.

Example:

```text
expected_state MERGE succeeds
metadata append fails
```

could leave state and metadata temporarily inconsistent.

Atlas accepts this limitation for the current learning/project scope.

A larger production platform could address this through stronger orchestration/recovery design.

---

## 51.2 Timestamp cutoff is weaker than source LSN watermark

Atlas currently uses:

```text
source_timestamp
```

to decide CDC interval membership.

A stronger production snapshot contract could carry the source database/WAL LSN at the moment the snapshot was taken.

Then reconciliation could process:

```text
(previous_lsn, snapshot_lsn]
```

instead of timestamp boundaries.

Current Atlas snapshots do not carry a source LSN watermark.

---

## 51.3 Bucket checksum implementation uses collection and sorting

Current bucket hashing conceptually uses:

```text
collect_list
sort_array
```

which can become memory-intensive if bucket count is too small for very large datasets.

Therefore:

```text
bucket_count
```

is configurable.

Atlas does not prematurely redesign this until scale measurements justify it.

---

## 51.4 Exception volume may not justify physical partitioning

Large source volume does not necessarily imply large reconciliation exception volume.

Therefore exception detail may contain only a few rows even when the source contains millions.

Atlas can retain a derived:

```text
snapshot_date
```

for filtering, but physical partitioning should be introduced only if measured query/write patterns justify it.

---

# 52. Current Non-Goals

The implemented reconciliation deliberately does not include:

```text
automatic repair
complex retry frameworks
reconciliation dashboards
cross-table transaction protocol
advanced distributed checksum tree
continuous reconciliation
automatic source mutation
full historical snapshot archive
sophisticated compaction framework
```

These are intentionally outside the current Customer-domain scope.

---

# 53. Operational Debugging Guide

When reconciliation produces unexpected mismatches, inspect in this order.

---

## Step 1 — Confirm Time Semantics

Check:

```text
snapshot_as_of
latest expected_state_metadata.state_as_of
source_timestamp of relevant CDC events
Spark timezone
```

Make sure all are UTC.

---

## Step 2 — Check Snapshot Row Count

If:

```text
snapshot_row_count = 0
```

but the CSV clearly contains data, inspect snapshot validation.

Example problem encountered for addresses:

```text
PostgreSQL boolean = t/f
Spark BooleanType parse = NULL
DQ rejects every row
```

---

## Step 3 — Inspect Expected State

Read:

```text
expected_state
```

and confirm that:

- updates are present,
- deletes are gone,
- inserts are present,
- ordering fields correspond to latest accepted event.

---

## Step 4 — Inspect Expected-State Metadata

Confirm:

```text
previous_state_as_of
state_as_of
processed_cdc_rows
records_inserted
records_updated
records_deleted
state_row_count
```

If `processed_cdc_rows = 0` when events should exist, suspect cutoff logic first.

---

## Step 5 — Inspect Incremental CDC Interval

Verify:

```text
previous_state_as_of < source_timestamp <= snapshot_as_of
```

for the expected events.

---

## Step 6 — Inspect Exception Detail

Use:

```text
reconciliation_status
mismatch_columns
snapshot_* values
cdc_* values
```

to determine the exact divergence.

---

# 54. Common Failure Interpretation

## Case A

```text
snapshot_row_count = 0
cdc_row_count > 0
```

Likely causes:

- snapshot DQ removed everything,
- wrong snapshot path/file,
- schema parse issue.

---

## Case B

```text
snapshot count = CDC count
exceptions > 0
```

The populations have equal size but different membership/state.

Inspect:

```text
missing_in_cdc
missing_in_snapshot
checksum_mismatch
```

---

## Case C

Expected state remains old after source updates.

Inspect:

```text
latest expected_state_metadata.state_as_of
source_timestamp
snapshot_as_of
```

Likely causes include:

- incorrect time zone,
- wrong interval,
- state metadata already advanced beyond the event timestamp.

---

## Case D

Only checksum mismatches occur.

Inspect:

```text
mismatch_columns
normalization rules
updated_at
case/whitespace differences
```

---

# 55. Reconciliation Function Responsibilities

The common reconciliation module owns reusable mechanics such as:

```text
attach_snapshot_metadata
get_snapshot_valid_data
get_cdc_valid_data
get_reconciliation_records
get_reconciliation_run_metrics
persist_reconciliation_results
persist_expected_state
persist_expected_state_metadata
get_latest_expected_state_as_of
get_incremental_cdc_changes
get_latest_incremental_cdc_changes
```

Entity-specific modules own:

```text
schema
DQ conditions
checksum normalization
entity key
comparison columns
snapshot path
Silver history path
orchestration
```

This keeps common mechanics reusable without moving business-specific logic into generic utilities.

---

# 56. Reconciliation Status Reference

| Status | Meaning |
|---|---|
| `MATCH` | Entity exists on both sides and normalized checksum matches |
| `CHECKSUM_MISMATCH` | Entity exists on both sides but business state differs |
| `MISSING_IN_CDC` | Entity exists in authoritative snapshot but not expected state |
| `MISSING_IN_SNAPSHOT` | Entity exists in expected state but not authoritative snapshot |
| `SUCCESS` | Reconciliation completed with zero exceptions |
| `COMPLETED_WITH_EXCEPTIONS` | Reconciliation completed but data mismatches were found |
| `FAILED` | Processing itself failed; handled at execution/orchestration level |

---

# 57. Final Data Flow

```text
                    POSTGRESQL
                        │
          ┌─────────────┴─────────────┐
          │                           │
          │                           │
 AUTHORITATIVE SNAPSHOT          DEBEZIUM CDC
          │                           │
          │                         KAFKA
          │                           │
          │                         BRONZE
          │                           │
          │                     SILVER CDC HISTORY
          │                           │
          │                           ▼
          │                  latest previous cutoff
          │                           │
          │                           ▼
          │            CDC in (previous_cutoff, T]
          │                           │
          │                           ▼
          │                 latest event per entity
          │                           │
          │                           ▼
          │                   DELTA MERGE
          │                           │
          │                           ▼
          │                    EXPECTED_STATE
          │                           │
          │                           ├────────► EXPECTED_STATE_METADATA
          │                           │
          └──────────────┬────────────┘
                         │
                         ▼
                  NORMALIZE + HASH
                         │
                         ▼
                  BUCKET COMPARISON
                         │
              ┌──────────┴──────────┐
              │                     │
            MATCH                MISMATCH
              │                     │
              │                     ▼
              │              ROW-LEVEL JOIN
              │                     │
              │                     ▼
              │               FIELD DIAGNOSTICS
              │                     │
              └──────────┬──────────┘
                         │
                         ▼
                CALCULATE RUN METRICS
                         │
              ┌──────────┴──────────┐
              │                     │
              ▼                     ▼
        RUN_SUMMARY          EXCEPTION_DETAIL
```

---

# 58. Decision Rationale

This design was chosen because it provides:

- an independent source-vs-CDC control,
- point-in-time correctness,
- auditability,
- incremental scalability,
- deterministic comparisons,
- clear failure diagnostics,
- reusable mechanics across Customer subdomains,
- minimal unnecessary persistence,
- production-relevant concepts without overbuilding the learning platform.

The design intentionally favors clarity and auditability over automatic correction.

---

# 59. Future Improvements

These are intentionally deferred.

Potential future improvements include:

1. Snapshot LSN/watermark instead of timestamp-only boundaries.
2. Stronger atomicity between expected state and metadata.
3. Better large-scale bucket checksum algorithms.
4. Physical partitioning only after measurements justify it.
5. Orchestration retries and reconciliation SLAs.
6. Monitoring/dashboarding.
7. Automated alerting for exception thresholds.
8. Periodic deep/full reconciliation in addition to incremental daily runs.
9. Shared CDC ordering-state optimization after Orders and Payments are implemented.
10. Optional controlled repair workflows with explicit approval and lineage.

---

# 60. Definition of Done for Customer Reconciliation

Customer reconciliation is considered complete because Atlas now supports:

- authoritative snapshots,
- customer/address/consent-specific snapshot validation,
- CDC-derived expected state,
- bootstrap state reconstruction,
- incremental CDC state advancement,
- create/update/delete MERGE semantics,
- expected-state metadata,
- deterministic row normalization,
- row checksums,
- bucket checksums,
- row-level mismatch drill-down,
- field-level mismatch diagnostics,
- missing-in-CDC detection,
- missing-in-snapshot detection,
- checksum mismatch detection,
- run-level metrics,
- append-only run summaries,
- exception-only persistence,
- UTC cutoff semantics,
- successful validation across Customers, Addresses, and Consents.

---

# 61. Short Mental Model

When returning to this code months later, remember the system with this sentence:

> **The snapshot tells Atlas what the source says exists at time T; expected state tells Atlas what CDC says should exist at T; reconciliation measures the difference and records it without silently repairing anything.**

And remember the four Delta datasets like this:

```text
expected_state
    → What does CDC currently say exists?

expected_state_metadata
    → How did expected state advance, and through what cutoff?

run_summary
    → Did CDC-derived state agree with the snapshot for this run?

exception_detail
    → Exactly which entities disagreed, and how?
```
