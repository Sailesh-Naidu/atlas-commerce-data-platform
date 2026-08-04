# ADR-003: Domain-Oriented Repository Structure

**Status:** Accepted  
**Date:** 2026-08-02  
**Owners:** Sailesh

---

## Context

Atlas Commerce Data Platform will contain multiple business domains,
including Customer, Product, Orders, Payments, Inventory, Shipping,
Returns, Promotions and Activity.

We need to decide how production code should be organized inside the
repository.

Two primary approaches were considered:

1. Organize code by processing layer, such as Bronze, Silver and Gold.
2. Organize code primarily by business domain, with responsibilities
   separated inside each domain.

This decision affects domain ownership, maintainability, testing,
team independence and long-term repository growth.

The Atlas blueprint requires business transformations to remain separate
from I/O adapters, shared utilities to remain small and generic, and
domain logic to stay within the owning domain. fileciteturn0file0L519-L527

---

## Decision

Atlas production code will be organized primarily by **business domain**.

Each domain will own its:

- contracts
- ingestion behavior
- transformations
- data-quality rules
- canonical Silver logic
- reconciliation logic
- domain-specific models

Generic technical capabilities may be placed inside a shared platform
layer named `common`.

One domain must not import another domain's private implementation.

Cross-domain interaction must happen through published contracts,
canonical tables or explicitly owned cross-domain data products.

---

## Target Structure

The long-term repository direction is:

```text
src/
└── atlas_platform/
    ├── common/
    │   ├── config/
    │   ├── logging/
    │   ├── metrics/
    │   ├── spark/
    │   ├── io/
    │   ├── quality/
    │   ├── datetime/
    │   └── notifications/
    │
    └── domains/
        ├── customer/
        │   ├── contracts/
        │   ├── ingestion/
        │   ├── transformations/
        │   ├── quality/
        │   ├── reconciliation/
        │   └── models/
        │
        ├── product/
        ├── order/
        ├── payment/
        └── inventory/