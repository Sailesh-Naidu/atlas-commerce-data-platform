# ADR-001: Domain-Driven Silver Ownership

**Status:** Accepted

**Date:** 2026-08-02

**Authors:** Sailesh

---

# Context

Atlas Commerce Data Platform consists of multiple business domains including Customer, Product, Orders, Payments, Inventory, Shipping and others.

A key architectural decision was determining ownership of the canonical Silver layer.

Two approaches were considered:

1. Central Silver ownership
2. Domain-owned canonical Silver

Since Silver represents the validated, canonical business state of a domain, ownership must be clearly defined before implementation begins.

---

# Decision

Atlas will adopt **Domain-Owned Canonical Silver**.

Each business domain owns:

- Bronze ingestion
- Canonical Silver tables
- Domain business rules
- Data quality rules
- Schema evolution
- Published data contracts

Other domains will consume **published contracts** instead of relying on another domain's internal implementation.

Example:

```
Customer Domain

Bronze
   ↓
Customer Silver
   ↓
Published Contract
   ↓
Orders
Payments
Marketing
AI
```

---

# Alternatives Considered

## Option A — Central Silver Ownership

A central platform layer owns all Silver tables regardless of business domain.

### Advantages

- Easier standardization
- Consistent naming conventions
- Shared validation logic
- Central governance

### Disadvantages

- Becomes a bottleneck as domains grow
- Central team must understand every business domain
- Reduced team autonomy
- Slower delivery
- Higher coordination overhead

---

## Option B — Domain-Owned Canonical Silver (Chosen)

Each domain owns its own canonical Silver tables and publishes stable contracts.

### Advantages

- Clear ownership
- Better maintainability
- Independent domain evolution
- Teams work within their business expertise
- Easier long-term scaling of engineering teams

### Disadvantages

- Possible duplication of technical utilities
- Risk of inconsistent standards
- Requires strong governance
- Contract versioning becomes important

---

# Consequences

Business logic remains inside each domain.

Reusable technical functionality belongs inside the shared platform layer.

Example:

Shared Platform

- Logging
- Metrics
- Configuration
- Spark utilities
- Generic validation framework

Customer Domain

- Customer segmentation
- Customer status
- Consent rules
- Customer business validations

Orders Domain

- Order lifecycle
- Order validations
- Order business rules

---

# Risks

- Duplicate implementations across domains
- Different schema conventions
- Contract drift
- Uneven engineering quality

These risks will be mitigated through:

- Engineering standards
- ADR reviews
- Shared platform utilities
- Stable published contracts
- Contract versioning
- Friday Architecture Reviews

---

# Out of Scope

Cross-domain business products do **not** belong inside canonical Silver.

Examples:

- Customer Lifetime Value
- Customer 360
- Revenue Analytics
- Order Conversion Metrics

These belong in Gold data products.

---

# Decision Summary

Atlas adopts Domain-Owned Canonical Silver because it provides clear ownership, improves maintainability, allows independent domain evolution, and scales better for long-term enterprise development than a centrally managed Silver layer.

---

# Related ADRs

- ADR-002 — Delta Lake as Initial Table Format

---

# Status History

| Date | Status | Notes |
|------|--------|-------|
| 2026-08-02 | Accepted | Initial architectural decision |