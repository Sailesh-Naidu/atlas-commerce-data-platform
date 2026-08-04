# Atlas Engineering Principles

Version: 1.0

Status: Active

Last Updated: 2026-08-02

---

# Purpose

This document defines the engineering principles that guide every
architectural decision, implementation, code review, and production
deployment in Atlas.

These principles exist to ensure consistency as the platform grows and
to provide a shared engineering philosophy for all contributors.

---

# Principle 1

## Business Logic Belongs to the Owning Domain

Business rules must remain inside the domain that owns the data.

Domains must never depend on another domain's internal implementation.

Cross-domain communication happens through published contracts and
well-defined data products.

---

# Principle 2

## Bronze Preserves Reality

Bronze stores exactly what Atlas received.

Bronze must never discard information simply because it appears
incorrect or duplicated.

Bronze exists for replay, auditing, debugging, lineage and recovery.

---

# Principle 3

## Silver Represents Business Truth

Silver contains the validated and canonical business state.

Only data that satisfies business rules should become part of
canonical Silver.

Duplicates, invalid records and unresolved events must not modify
business state.

---

# Principle 4

## Gold Exists for Consumers

Gold is not a dumping ground for random aggregations.

Every Gold dataset must have:

- Business owner
- Consumers
- Business purpose
- Refresh expectation
- Metric definitions

---

# Principle 5

## Configuration Drives Behaviour

Changing environments should require configuration changes rather than
code changes.

Environment-specific values must never be hardcoded inside business
logic.

---

# Principle 6

## Build for Replay

Every pipeline should be replayable.

Atlas must be capable of rebuilding business state from trusted source
data whenever practical.

Replayability takes precedence over implementation convenience.

---

# Principle 7

## Idempotency is Mandatory

Processing the same event multiple times must not change the final
business state.

Duplicate events should be visible through operational metrics but must
not create duplicate business records.

---

# Principle 8

## Shared Code Must Be Domain Neutral

The shared platform layer contains only generic technical capabilities.

Business rules must never be moved into shared utilities simply because
multiple domains perform similar operations.

---

# Principle 9

## Prefer Simplicity Before Generalisation

Do not introduce abstractions before multiple real use cases exist.

Avoid speculative frameworks and premature optimisation.

Generalise only after repeated patterns emerge.

---

# Principle 10

## Every Important Decision is Documented

Architectural decisions must be captured using ADRs.

Code should implement documented decisions rather than silently define
architecture.

---

# Principle 11

## Observability is a Feature

Every production pipeline must expose sufficient logging, metrics and
diagnostics to understand its behaviour without modifying code.

If a failure cannot be diagnosed from telemetry, the platform is
considered incomplete.

---

# Principle 12

## Data Quality is Continuous

Data quality is not a single validation step.

Validation, monitoring, reconciliation and alerting are continuous
responsibilities throughout the pipeline.

---

# Principle 13

## Security and Governance are Built In

Data governance, ownership, lineage, masking, retention and access
control are platform capabilities rather than optional additions.

---

# Principle 14

## Measure Before Optimising

Performance optimisation must be based on evidence.

Every optimisation should have a measurable reason and measurable
benefit.

---

# Principle 15

## Design for Evolution

Atlas is expected to evolve.

Repository structure, contracts and platform components should support
future technologies without unnecessary rewrites.

Examples include:

- Airflow
- dbt
- Snowflake
- Kubernetes
- Unity Catalog
- AI Feature Store
- Vector Databases

These should extend the platform rather than replace it.

---

# Principle 16

## Teach Through the Codebase

The repository should be understandable by engineers joining the
project.

Documentation, naming, tests and architecture should explain not only
how the system works but why it was designed that way.