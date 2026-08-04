# TS-001: Atlas Testing Strategy

Status: Accepted

## Rule

Atlas uses layered testing covering unit logic, Spark transformations,
contracts, integrations, end-to-end flows, data quality, idempotency,
recovery, reconciliation and performance.

## Principles

- Tests must be deterministic.
- Tests must verify data content and schema, not only row counts.
- Critical pipelines must prove idempotency.
- Recovery behavior must be tested intentionally.
- Test data must be reproducible.
- Performance improvements require benchmarks.
- Coverage percentage alone does not prove quality.

## Pull Request Requirements

Every pull request must pass:

- Ruff checks
- relevant unit tests
- relevant transformation tests
- relevant contract tests

Integration and end-to-end tests are required when the change crosses
component boundaries.

## Rationale

This strategy protects business correctness, supports safe refactoring,
and verifies that Atlas can recover from realistic production failures.