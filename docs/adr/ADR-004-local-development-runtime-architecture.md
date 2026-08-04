# ADR-004: Local Development and Runtime Architecture

**Status:** Accepted

**Date:** 2026-08-02

**Authors:** Sailesh, ChatGPT

---

# Context

Atlas Commerce Data Platform is initially developed by a single engineer on
a local workstation before evolving into a distributed cloud-native
platform.

The local development environment must:

- Support rapid development and debugging.
- Closely resemble production architecture where practical.
- Minimize unnecessary infrastructure complexity.
- Allow gradual migration to cloud platforms such as Databricks.
- Enable learning of distributed data engineering concepts without
  overwhelming operational overhead.

The local runtime architecture must also provide a clear separation
between application code, infrastructure services, storage systems and
developer tooling.

---

# Decision

Atlas will adopt a **hybrid local architecture**.

Business logic and Spark will execute directly on the developer machine,
while infrastructure services will execute inside Docker Compose.

The platform will evolve gradually from a lightweight local environment
towards a production-like distributed architecture.

---

# Why Now

The runtime architecture must be defined before repository creation
because it determines:

- repository structure
- configuration design
- local development workflow
- storage layout
- deployment strategy
- testing approach
- infrastructure provisioning

Changing the runtime architecture after implementation would introduce
unnecessary migration effort.

---

# Runtime Architecture

The local runtime consists of four major layers.

```text
                Developer Workstation

-------------------------------------------------------

Developer Tools

PyCharm
Jupyter
Git
uv
pytest

-------------------------------------------------------

Application Runtime

Atlas Python Package
Spark local[*]
Spark UI

-------------------------------------------------------

Infrastructure (Docker Compose)

Kafka
Kafka UI
PostgreSQL
MinIO

-------------------------------------------------------

Storage

Local Delta Lake (Stage A)

↓

MinIO Object Storage (Stage B)

↓

Cloud Object Storage (Future)
```

---

# Component Responsibilities

## Spark

Spark executes directly on the local machine.

Reasons:

- Faster debugging.
- Native IDE integration.
- Easier breakpoint support.
- Simpler dependency management.
- Better learning experience.

Future stages will introduce distributed Spark environments.

---

## Docker Compose

Docker Compose hosts infrastructure services only.

Services include:

- Kafka
- Kafka UI
- PostgreSQL
- MinIO

This keeps infrastructure isolated while allowing application code to run
natively.

---

## Delta Lake Storage

Stage A

Delta tables are stored on the local filesystem.

Example

```
./data/lakehouse/
```

Reasons:

- Easier debugging
- Easy inspection of Parquet files
- Easy inspection of _delta_log
- Minimal configuration

Stage B

Storage migrates to MinIO.

Example

```
s3a://atlas-lakehouse/
```

Stage C

Cloud object storage.

Examples:

- Amazon S3
- Azure Data Lake Storage

---

## PostgreSQL

PostgreSQL serves two purposes.

### Operational System Simulation

Separate schemas (or logical databases) simulate independent operational
services.

Examples:

- customer_service
- product_service
- order_service
- payment_service
- inventory_service

### Platform Operational Metadata

PostgreSQL may temporarily store platform operational metadata where
appropriate.

Canonical Bronze, Silver and Gold datasets remain in Delta Lake.

---

## Kafka

Atlas begins with a single Kafka broker.

Reasons:

- Lower infrastructure complexity.
- Lower resource consumption.
- Supports all required streaming concepts.
- Sufficient for local development.

Future stages introduce:

- Multiple brokers
- Replication
- Leader election
- Broker failover
- High availability

---

# Configuration Strategy

Atlas configuration follows a layered approach.

```text
Base Configuration

↓

Environment Override

↓

Environment Variables

↓

Validated Runtime Settings
```

Configuration categories include:

- Application
- Spark
- Kafka
- Storage
- PostgreSQL
- Logging
- Metrics
- Pipeline Settings

Secrets are never committed to source control.

Local development uses:

```
.env
```

The repository contains only:

```
.env.example
```

---

# Logging Strategy

Atlas uses structured JSON logging.

Every log automatically contains:

- timestamp
- environment
- application
- domain
- pipeline
- run_id
- batch_id
- log level
- message
- application version

Pipeline code should not manually construct these fields.

Logging focuses on:

- lifecycle events
- validation summaries
- pipeline milestones
- failures
- performance information

Record-level failures belong in quarantine datasets rather than logs.

---

# Metrics Strategy

Atlas exposes operational metrics rather than individual records.

Examples:

- records_read
- records_written
- duplicate_records
- invalid_records
- pipeline_duration
- throughput
- data_quality_failures

Metrics support future integration with Prometheus and Grafana.

---

# Storage Ownership

Delta Lake owns:

- Bronze
- Silver
- Gold
- Quarantine
- Operational data products

PostgreSQL owns:

- Operational source simulation
- Platform operational metadata

---

# Development Workflow

Developers use:

- PyCharm
- Jupyter
- Git
- uv
- pytest

Production business logic resides inside Python packages.

Jupyter notebooks are used only for:

- experimentation
- exploration
- debugging
- learning

Production code must never depend on notebooks.

---

# Evolution Roadmap

Stage A

- Local Spark
- Local Delta
- Single Kafka Broker

↓

Stage B

- MinIO
- Object Storage
- Improved Observability

↓

Stage C

- Distributed Spark
- Multi-Broker Kafka

↓

Stage D

- Databricks
- Unity Catalog

↓

Stage E

- Cloud Production Platform

---

# Alternatives Considered

## Full Docker Environment

Advantages

- Closer to production.
- Consistent runtime.

Disadvantages

- Harder debugging.
- Slower development.
- Higher learning complexity.

---

## Fully Local Environment

Advantages

- Very simple.

Disadvantages

- Poor production similarity.
- Difficult infrastructure management.
- Limited object storage simulation.

---

## Hybrid Architecture (Chosen)

Advantages

- Fast local development.
- Easier debugging.
- Production-inspired architecture.
- Clear separation between infrastructure and application.
- Gradual migration path.

Disadvantages

- Local Spark differs from production clusters.
- Initial filesystem storage differs from cloud object storage.
- Single Kafka broker does not simulate replication.

---

# Risks

- Local behaviour differs from distributed production.
- Object storage semantics are introduced later.
- High availability is not represented initially.

These risks are accepted because the architecture evolves in stages.

---

# Review Triggers

Review this ADR when:

- Spark moves to a distributed cluster.
- MinIO replaces local storage.
- Databricks becomes the execution platform.
- Kubernetes becomes the deployment target.
- Multi-broker Kafka is introduced.
- Production infrastructure differs significantly from local.

---

# Decision Summary

Atlas adopts a hybrid local runtime architecture in which business logic
and Spark execute natively while infrastructure services execute in
Docker Compose.

This approach maximizes developer productivity, supports incremental
learning, and provides a smooth migration path towards a production-grade
cloud-native platform.

---

# Related ADRs

- ADR-001 — Domain-Owned Canonical Silver
- ADR-002 — Delta Lake as Initial Lakehouse Table Format
- ADR-003 — Domain-Oriented Repository Structure

---

# Status History

| Date | Status | Notes |
|------|--------|-------|
| 2026-08-02 | Accepted | Initial local runtime architecture |