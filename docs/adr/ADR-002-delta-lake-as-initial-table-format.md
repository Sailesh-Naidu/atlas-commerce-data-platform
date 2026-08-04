# ADR-002: Delta Lake as the Initial Lakehouse Table Format

**Status:** Accepted

**Date:** 2026-08-02

**Authors:** Sailesh

---

# Context

Atlas Commerce Data Platform requires a transactional storage layer for
batch and streaming data processing.

The platform must support:

- Batch ingestion
- Streaming ingestion
- Change Data Capture (CDC)
- Slowly Changing Dimensions (SCD)
- Schema evolution
- Idempotent processing
- Time travel
- Recovery after failures
- Reliable concurrent reads and writes

The available options considered were:

1. Plain Parquet
2. Delta Lake
3. Apache Iceberg
4. Apache Hudi

Since this decision impacts every domain and every pipeline, it must be
made before implementation begins.

---

# Decision

Atlas will use **Delta Lake** as the primary table format for the initial
implementation.

Delta Lake provides the transactional capabilities required for our
Spark-first architecture while integrating naturally with batch,
Structured Streaming and future Databricks deployments.

---

# Why Now

This decision is required before implementation because the storage
format determines:

- Table creation
- Read and write patterns
- CDC implementation
- SCD implementation
- Recovery strategy
- Streaming architecture
- Repository design
- Future deployment

Changing the table format later would require migration effort and
possible data conversion.

---

# Business Requirements

Atlas requires support for:

- Reliable updates
- Reliable deletes
- MERGE operations
- CDC processing
- Streaming writes
- Schema evolution
- Recovery after failures
- Historical data access
- Replayable pipelines

---

# Alternatives Considered

## Option A — Plain Parquet

### Advantages

- Open standard
- High performance
- Supported by many processing engines
- Simple storage format

### Disadvantages

- No ACID transactions
- No native MERGE support
- No UPDATE support
- No DELETE support
- No transaction log
- No built-in time travel
- Limited support for enterprise CDC workflows
- Application must manage consistency

---

## Option B — Delta Lake (Chosen)

### Advantages

- ACID transactions
- Native MERGE support
- UPDATE and DELETE operations
- Transaction log
- Time travel
- Schema evolution
- Excellent Spark integration
- Strong Structured Streaming integration
- Supports idempotent processing
- Simplifies CDC implementation
- Simplifies SCD implementation

### Disadvantages

- Greater coupling to the Delta ecosystem
- Migration to another table format requires planning
- Some maintenance operations are Delta-specific

---

## Option C — Apache Iceberg

### Advantages

- Open table format
- Excellent multi-engine support
- Strong metadata architecture
- Good schema evolution
- Hidden partitioning

### Disadvantages

- Not selected for the initial implementation
- Would increase learning complexity during early project phases
- Atlas currently focuses on Spark-first development

---

## Option D — Apache Hudi

### Advantages

- Strong incremental processing
- Good support for CDC workloads
- Efficient upsert capabilities

### Disadvantages

- Less aligned with Atlas learning roadmap
- Additional complexity without immediate business benefit

---

# Consequences

All Bronze, Silver and Gold tables will initially use Delta Lake.

Future domains will implement:

- MERGE
- UPDATE
- DELETE
- Schema evolution
- Time travel
- CDC
- SCD
- Streaming

using Delta Lake.

Iceberg will be introduced later in the project to compare design,
performance, interoperability and operational trade-offs.

---

# Risks

## Vendor / Ecosystem Coupling

Atlas becomes more closely aligned with Delta APIs and maintenance
operations.

Mitigation:

- Separate business logic from storage implementation.
- Keep Bronze replayable.
- Evaluate Iceberg in a later project phase.

---

## Future Migration Cost

Migrating to another table format may require:

- Table conversion
- Code changes
- Metadata migration
- Replay from Bronze if required

Mitigation:

- Maintain replayable Bronze data.
- Isolate storage-specific logic.
- Document migration strategy before adoption.

---

# Review Trigger

Review this ADR if:

- Atlas adopts a multi-engine architecture.
- Iceberg becomes the primary table format.
- Storage requirements change significantly.
- Cloud platform strategy changes.
- Spark is no longer the primary processing engine.

---

# Decision Summary

Delta Lake is selected as the initial table format because it satisfies
the transactional, streaming and recovery requirements of Atlas while
providing excellent integration with Spark and a smooth migration path
to Databricks and Unity Catalog.

Apache Iceberg will be evaluated later as part of the platform evolution
rather than during the initial implementation.

---

# Related ADRs

- ADR-001 — Domain-Driven Silver Ownership

---

# Status History

| Date | Status | Notes |
|------|--------|-------|
| 2026-08-02 | Accepted | Initial architectural decision |