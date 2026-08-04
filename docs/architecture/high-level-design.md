# Atlas Commerce Data Platform — High-Level Design

## 1. Business Context

Atlas Commerce Data Platform is an enterprise data platform that
transforms data generated across the Atlas marketplace into trusted,
governed, and reusable business data products.

It enables operational monitoring, customer and product analytics,
revenue and inventory analysis, and future AI capabilities such as
recommendations and intelligent customer experiences.

## 2. Platform Consumers

Atlas provides trusted data products to multiple business and technical
consumers.

### Business Analysts

Business analysts use Atlas to analyze revenue, profitability,
customer behavior, sales trends, KPIs and business performance through
trusted analytical datasets.

### Operations Teams

Operations teams use Atlas to monitor inventory, supply chain,
payments, reconciliation, operational SLAs and platform health to
support day-to-day business operations.

### AI and Data Science Teams

AI and Data Science teams consume trusted data products for feature
engineering, customer segmentation, recommendation systems,
forecasting, fraud detection and future intelligent applications.

## 3. Source Systems

Atlas ingests data from the operational systems that run the Atlas
marketplace.

The Customer Service generates customer data when a customer registers,
updates profile details, changes consent preferences, or changes status.

The Order Service generates order data when customers place, confirm,
cancel, ship, or complete orders.

The Product and Seller services generate catalog, pricing, product
attribute, supplier, contract, and seller information.

## 4. Ingestion Architecture

Atlas supports multiple ingestion patterns based on domain freshness,
correctness, and reconciliation requirements.

Customer and Product use CDC to maintain near-real-time canonical state,
supported by periodic authoritative snapshots for reconciliation.

Orders and Payments publish lifecycle events because their state changes
must be processed with low latency.

Inventory publishes stock-movement events and also provides periodic
snapshots of authoritative on-hand inventory. This allows Atlas to
detect missing, duplicated, or out-of-order inventory events.

External APIs and partner files may act as source interfaces, but all
data enters Atlas through governed batch, CDC, or event-ingestion
patterns.

## 5. Domain Architecture and Ownership

Atlas follows a domain-oriented architecture in which each business
domain owns its complete lifecycle from ingestion through canonical
representation.

The initial business domains are:

- Customer
- Product
- Orders
- Payments
- Inventory

Each domain owns:

- ingestion contracts
- schema evolution
- canonical Silver datasets
- business transformations
- data-quality rules
- reconciliation logic
- domain-specific metadata

Shared technical capabilities such as configuration, logging, Spark
utilities, metrics, storage adapters, and generic validation are placed
inside the shared `common` platform layer.

Business logic must never be implemented inside `common`.

Cross-domain interaction must occur only through published contracts,
canonical datasets, or approved Gold data products.

Domains must not directly depend on another domain's internal
implementation.

This ownership model enables independent evolution, simplifies code
reviews, reduces coupling, and allows teams to scale without creating a
central transformation layer.

The repository therefore separates:

- business domains
- platform capabilities
- executable jobs

Jobs orchestrate execution while domains own business logic.

The dependency direction is:

```text
Jobs
    ↓
Domains
    ↓
Common Platform
```

The reverse dependency is not permitted.

This architecture establishes clear ownership boundaries while allowing
the platform to evolve as additional business domains are introduced.

## 6. End-to-End Data Flow

Atlas transforms operational business events into trusted analytical
data products through a layered architecture.

The end-to-end flow is:

```text
Operational Systems
        │
        ▼
Batch / CDC / Events
        │
        ▼
Bronze
        │
        ▼
Silver
        │
        ▼
Gold
        │
        ├── Business Analytics
        ├── Operations
        ├── Finance
        └── AI & Machine Learning
```

Each layer has a single well-defined responsibility.

### Operational Systems

Operational systems are the systems of record that execute business
transactions.

Examples include:

- Customer Service
- Product Service
- Order Service
- Payment Service
- Inventory Service

Atlas does not own operational transactions. It consumes operational
data and transforms it into governed analytical datasets.

### Ingestion Layer

The ingestion layer receives data through:

- Batch files
- Change Data Capture (CDC)
- Business events
- External APIs

Every record entering Atlas receives ingestion metadata to support
lineage, auditing and replay.

### Bronze Layer

Bronze preserves exactly what Atlas received from upstream systems.

Bronze:

- stores raw events and snapshots
- preserves duplicates
- preserves out-of-order events
- stores ingestion metadata
- enables replay and auditing

Bronze performs only minimal technical validation required for reliable
storage.

### Silver Layer

Silver represents the trusted business state.

Silver performs:

- schema validation
- data type normalization
- deduplication
- business validation
- canonical transformations
- CDC application
- reconciliation
- quarantine routing

Silver datasets are owned by their respective business domains and act
as the canonical interface for downstream consumers.

### Gold Layer

Gold contains business-oriented data products created for specific
consumers.

Examples include:

- Customer 360
- Revenue Analytics
- Product Performance
- Inventory Health
- Payment Reliability

Every Gold data product must have:

- a business owner
- identified consumers
- documented business purpose
- defined freshness expectations
- documented metric definitions

Gold datasets are optimized for consumption rather than operational
processing.

## 7. Bronze Layer Architecture

The Bronze layer is the immutable landing zone for all data entering
Atlas.

Its primary responsibility is to preserve the exact state of incoming
data while providing sufficient metadata to support replay,
troubleshooting, auditing and lineage.

Bronze is the system's historical record of what was received rather
than what is considered correct.

### Objectives

The Bronze layer exists to:

- preserve source fidelity
- support replay
- support auditing
- enable troubleshooting
- provide lineage
- decouple ingestion from business processing

### Responsibilities

Bronze stores:

- raw events
- CDC records
- batch snapshots
- ingestion metadata
- source metadata
- schema version information

Bronze intentionally preserves:

- duplicate events
- out-of-order events
- late-arriving events
- unexpected payloads

No business decisions are made inside Bronze.

### Metadata

Every Bronze record should contain sufficient metadata to trace the
record throughout the platform.

Typical metadata includes:

- source system
- ingestion timestamp
- event timestamp
- event identifier
- batch identifier
- schema version
- ingestion run identifier

Streaming records additionally preserve metadata such as:

- Kafka topic
- partition
- offset

Batch records additionally preserve metadata such as:

- source filename
- extract timestamp

### Allowed Operations

Bronze performs only technical processing required for reliable storage.

Examples include:

- schema parsing
- metadata enrichment
- format conversion
- partition assignment
- write validation

Business validation is intentionally deferred to Silver.

### Not Allowed

The following operations are prohibited within Bronze:

- business-rule validation
- deduplication
- CDC merge logic
- customer segmentation
- metric calculation
- aggregation
- canonical transformations

Moving these operations into Bronze would reduce replayability and blur
layer responsibilities.

### Storage Strategy

Bronze is append-only.

Existing source records are never modified.

Corrections from upstream systems arrive as new events or new snapshot
records rather than updates to existing Bronze data.

This guarantees a complete historical record of all received data.

### Replay

Because Bronze preserves source fidelity, any downstream layer can be
rebuilt by replaying Bronze data.

Replay supports:

- recovery after failures
- code changes
- schema evolution
- historical backfills
- platform migration

Replayability is a core engineering principle of Atlas and strongly
influences all downstream architectural decisions.

## 8. Silver Layer Architecture

The Silver layer represents the canonical business state of the Atlas
platform.

Unlike Bronze, which preserves the exact source history, Silver
contains validated, standardized and trusted business data that is safe
for downstream consumption.

Silver acts as the single source of truth for each business domain.

Every downstream consumer, including Gold data products, analytics,
machine learning, reconciliation and operational reporting, consumes
data from Silver rather than directly from Bronze.

---

### Objectives

The Silver layer exists to:

- create canonical business datasets
- validate incoming data
- standardize schemas
- normalize data types
- eliminate duplicate business events
- apply CDC changes
- maintain historical records where required
- quarantine invalid records
- provide stable interfaces to downstream consumers

---

### Domain Ownership

Each business domain owns its canonical Silver datasets.

Initial domains include:

- Customer
- Product
- Orders
- Payments
- Inventory

Each domain owns:

- business transformations
- canonical schema
- business validation rules
- data-quality rules
- CDC processing
- historical management
- reconciliation logic

No domain may modify another domain's canonical Silver datasets.

---

### Responsibilities

Silver performs business-aware processing including:

- schema validation
- data type normalization
- null handling
- business validation
- duplicate detection
- event ordering
- CDC processing
- Slowly Changing Dimension (SCD) handling
- referential validation
- quarantine routing

Unlike Bronze, Silver is responsible for determining whether incoming
data should become part of the trusted business state.

---

### Canonical Data Model

Every Silver dataset follows a canonical schema owned by its business
domain.

Canonical datasets provide:

- stable column names
- standardized data types
- consistent business definitions
- versioned contracts
- predictable interfaces

Canonical schemas isolate downstream consumers from changes in source
systems.

---

### Idempotency

Silver processing must be idempotent.

Processing the same business event multiple times must always produce
the same final business state.

Duplicate events must:

- remain preserved in Bronze
- be ignored by canonical Silver
- be visible through operational metrics

Idempotency is typically enforced using business identifiers such as:

- event_id
- transaction_id
- business keys

---

### CDC Processing

Silver applies Change Data Capture events to maintain the latest
canonical representation of business entities.

Typical CDC operations include:

- INSERT
- UPDATE
- DELETE

Business delete handling depends on domain requirements and may include:

- soft deletes
- hard deletes
- historical retention

CDC logic belongs entirely within the owning domain.

---

### Historical Data

Some business entities require historical tracking.

Examples include:

- customer status changes
- product price history
- customer consent history

Historical management is implemented using the appropriate strategy
for the domain, including SCD Type 1 or SCD Type 2 where applicable.

---

### Data Quality

Silver enforces business data quality.

Validation may include:

- mandatory field validation
- business rule validation
- duplicate detection
- reference validation
- value range validation
- format validation

Records failing validation do not become part of canonical Silver.

---

### Quarantine

Invalid records are routed to quarantine rather than silently discarded.

Quarantine records preserve:

- raw payload
- failure reason
- validation stage
- run identifier
- ingestion timestamp
- source metadata

Quarantine enables investigation, correction and replay without losing
source information.

---

### Reconciliation

Silver periodically reconciles its canonical state against authoritative
sources.

Typical reconciliation compares:

- CDC-derived state
- authoritative snapshots

Differences may indicate:

- missing events
- duplicate events
- out-of-order events
- upstream inconsistencies

Reconciliation improves long-term correctness while preserving
streaming performance.

---

### Consumer Contract

Silver provides stable, documented interfaces for downstream consumers.

Consumers must not depend directly on:

- Bronze schemas
- source-system schemas
- operational databases

Instead they consume canonical Silver datasets owned by the appropriate
business domain.

---

### Design Principles

The Silver layer follows these principles:

- Business truth over source fidelity
- Domain ownership
- Idempotent processing
- Replayability
- Stable contracts
- Continuous data quality
- Independent domain evolution

Silver is the foundation upon which every Gold data product in Atlas is
built.

## 9. Gold Layer Architecture

The Gold layer contains business-oriented data products designed for
specific consumers.

Unlike Silver, which represents canonical business truth, Gold organizes
that truth into datasets optimized for business reporting, operational
decision making and artificial intelligence workloads.

Gold is the presentation layer of the Atlas platform.

---

### Objectives

The Gold layer exists to:

- deliver trusted business data products
- provide optimized datasets for analytics
- support operational dashboards
- simplify business reporting
- enable machine learning and AI workloads
- provide a semantic business layer

Gold transforms canonical business data into information that directly
answers business questions.

---

### Responsibilities

Gold performs business-oriented transformations such as:

- aggregations
- KPI calculations
- dimensional modelling
- business metric calculations
- feature engineering
- consumer-specific optimizations

Gold must never become another source of business truth.

Its responsibility is to present trusted information already established
by Silver.

---

### Data Products

Every Gold dataset is treated as a managed data product.

Each data product must define:

- business owner
- technical owner
- consumers
- business purpose
- refresh frequency
- SLA
- SLI
- metric definitions

Gold datasets are products rather than temporary analytical outputs.

---

### Initial Gold Data Products

The initial release of Atlas includes business data products such as:

- Customer 360
- Revenue Analytics
- Product Performance
- Inventory Health
- Payment Reliability
- Sales Dashboard

Additional data products may be introduced as business requirements
evolve.

---

### Consumer Groups

Gold serves multiple consumer types.

Business Intelligence

Provides datasets optimized for dashboards, reporting and executive
analytics.

Operations

Provides operational datasets used to monitor inventory, payments,
pipeline health and business SLAs.

Finance

Provides trusted datasets for reconciliation, settlements and financial
reporting.

Artificial Intelligence

Provides curated datasets suitable for feature engineering, model
training, recommendation systems and future AI applications.

---

### Business Metrics

Business metrics are defined once within Gold and reused consistently
across all consumers.

Examples include:

- Total Revenue
- Gross Merchandise Value (GMV)
- Average Order Value
- Customer Lifetime Value
- Inventory Turnover
- Payment Success Rate

Metric definitions must be documented to ensure consistency across the
organization.

---

### Performance

Gold datasets may be optimized for read performance using techniques
such as:

- partitioning
- clustering
- precomputed aggregations
- consumer-specific modelling

These optimizations must never change business meaning.

---

### Ownership

Each Gold data product has a clearly defined business owner and
technical owner.

Ownership includes responsibility for:

- correctness
- documentation
- SLA compliance
- lifecycle management
- consumer communication

---

### Design Principles

The Gold layer follows these principles:

- Consumer-first design
- Business-oriented modelling
- Reusable data products
- Documented business metrics
- Stable interfaces
- Governed ownership

Gold is the final presentation layer of Atlas and represents the
trusted interface between the data platform and its consumers.

## 10. Data Quality, Quarantine, Replay and Reconciliation

Atlas is designed with the assumption that operational data may be
incomplete, duplicated, delayed, out-of-order or inconsistent.

Rather than silently accepting or discarding invalid data, Atlas
implements a governed framework for validation, quarantine,
reconciliation and replay.

These capabilities ensure that the platform maintains trusted business
data while preserving complete operational history.

---

### Data Quality

Data quality is a continuous responsibility rather than a single
validation step.

Validation occurs throughout the platform lifecycle and includes both
technical validation and business validation.

Technical validation verifies that incoming data can be processed safely.

Examples include:

- schema validation
- data type validation
- mandatory field validation
- malformed record detection

Business validation verifies that data satisfies business rules.

Examples include:

- customer status validation
- payment status validation
- inventory availability
- referential integrity
- business key uniqueness

Validation failures do not silently disappear.

Every validation outcome is recorded for operational visibility.

---

### Data Quality Framework

Atlas categorizes validation into multiple levels.

#### Level 1 — Technical Validation

Verifies that records are technically processable.

Typical checks include:

- schema compatibility
- mandatory columns
- supported data types
- corrupt payload detection

#### Level 2 — Business Validation

Verifies business correctness.

Typical checks include:

- business rule validation
- domain constraints
- reference validation
- duplicate business events

#### Level 3 — Operational Validation

Evaluates pipeline health.

Examples include:

- freshness thresholds
- duplicate percentage
- invalid record percentage
- reconciliation mismatches

Operational validation supports monitoring and alerting.

---

### Quarantine

Records failing validation are routed to quarantine rather than being
discarded.

Quarantine preserves failed records together with sufficient metadata
to support investigation and replay.

Each quarantined record contains information such as:

- raw payload
- failure reason
- validation stage
- source system
- ingestion timestamp
- pipeline run identifier
- event identifier

Quarantine is considered part of the operational platform rather than
a permanent storage layer.

Records remain available until they are corrected, replayed or archived
according to platform retention policies.

---

### Replay

Replay is a fundamental capability of Atlas.

The platform must be capable of rebuilding downstream datasets from
trusted source data without requiring manual reconstruction.

Replay supports:

- platform recovery
- historical backfills
- schema evolution
- code changes
- disaster recovery
- platform migration

Replay is performed from Bronze because Bronze preserves complete
source fidelity.

Replay must produce deterministic business results when processing the
same source data multiple times.

---

### Idempotency

Atlas pipelines must be idempotent.

Processing the same event multiple times must not change the final
business state.

Duplicate events are expected in distributed systems and are treated as
normal operational behaviour.

Typical idempotency identifiers include:

- event_id
- transaction_id
- business keys

Duplicate events remain preserved within Bronze while canonical Silver
prevents duplicate business updates.

Operational metrics record duplicate processing for observability.

---

### Reconciliation

Streaming systems cannot assume perfect event delivery.

Atlas periodically reconciles canonical Silver datasets against
authoritative business sources.

Examples include:

- Customer CDC versus daily customer snapshot
- Inventory events versus warehouse inventory snapshot
- Payment events versus settlement reports

Reconciliation identifies:

- missing events
- duplicate events
- out-of-order processing
- upstream inconsistencies
- historical divergence

Differences are investigated and corrected through controlled replay
or approved reconciliation procedures.

---

### Recovery Strategy

Atlas is designed to recover safely from failures.

Recovery includes:

- restarting interrupted pipelines
- replaying Bronze data
- reprocessing historical batches
- rebuilding Silver datasets
- regenerating Gold data products

Recovery procedures prioritize business correctness over processing
speed.

---

### Design Principles

The platform follows these principles:

- Preserve source history.
- Validate continuously.
- Never silently discard business data.
- Quarantine rather than delete.
- Replay instead of manually repairing data.
- Design for deterministic recovery.
- Measure data quality continuously.
### Data Quality Severity Classification

Not every validation failure requires the same operational response.

Atlas classifies data quality issues according to their business impact
and defines a standard handling strategy for each severity level.

| Severity | Description | Platform Response |
|-----------|-------------|-------------------|
| Critical | Business correctness cannot be guaranteed. Processing invalid data would compromise canonical business state. | Stop the pipeline immediately, generate an ERROR, and notify Operations. |
| High | Significant data quality issue affecting part of the dataset but not the entire pipeline. | Quarantine affected records, continue processing valid records where safe, and raise alerts for investigation. |
| Medium | Recoverable anomaly that does not compromise business correctness. | Continue processing, generate WARN logs, record operational metrics, and monitor trends. |
| Low | Informational issue with minimal business impact. | Generate INFO or DEBUG logs and include the event in operational reporting. |

Severity classification ensures that all Atlas pipelines respond to data
quality issues consistently regardless of implementation details.

Business domains may define their own validation rules, but the
operational response must follow the platform-wide severity framework.

Examples include:

**Critical**

- Required business identifier missing
- Corrupt source file
- Unsupported schema version
- Mandatory configuration unavailable

**High**

- Validation threshold exceeded
- Large duplicate percentage
- Referential integrity failures
- Unexpected business

## 11. Observability Architecture

Observability is a required platform capability of Atlas.

The platform must provide enough telemetry to detect, diagnose and
recover from failures without modifying application code or manually
reproducing the issue.

Atlas observability consists of:

- structured logs
- operational metrics
- data-quality metrics
- business metrics
- dashboards
- alerts
- audit metadata

### Observability Objectives

Atlas observability must answer:

- Is the pipeline running?
- Is data arriving?
- Is processing keeping up with input?
- Is the resulting data fresh?
- Are records being rejected or quarantined?
- Is state growing unexpectedly?
- Is the target storage responding normally?
- Which run, batch or streaming query is affected?
- What business data products are impacted?

### Pipeline Metrics

Every pipeline exposes:

- pipeline status
- start time
- completion time
- processing duration
- run identifier
- code version
- input record count
- output record count
- rejected record count
- retry count
- latest successful processing time
- data freshness

### Streaming Metrics

Streaming pipelines additionally expose:

- latest source offset
- latest processed offset
- consumer lag
- input rows per second
- processed rows per second
- micro-batch duration
- trigger interval
- watermark
- late-event count
- state-store rows
- state-store size
- sink commit latency
- streaming query identifier

### Spark Runtime Metrics

Atlas monitors:

- job and stage duration
- task duration
- failed tasks
- shuffle read and write
- memory spill
- disk spill
- garbage collection time
- executor loss
- skewed tasks

### Delta and Storage Metrics

Atlas monitors:

- table size
- file count
- average file size
- files added
- files removed
- write duration
- merge metrics
- compaction effectiveness
- transaction version

### Data-Quality Metrics

Atlas records:

- invalid record count
- duplicate record count
- quarantine count
- null and schema failure counts
- referential misses
- late-event count
- reconciliation variance
- threshold breaches

### Business Metrics

Business metrics provide visibility into the business impact of the
platform.

Examples include:

- order rate
- payment success rate
- inventory mismatch rate
- customer freshness
- fulfilment SLA breaches

### Structured Logging

Atlas uses structured JSON logging.

Logs include common operational context such as:

- environment
- domain
- pipeline
- run identifier
- batch identifier
- streaming query identifier
- event or correlation identifier
- code version
- log level
- human-readable message

Atlas logs lifecycle milestones, recoverable anomalies and failures.

Sensitive customer information, secrets and raw personal data must not
be written to logs.

### Dashboards

Dashboards provide views for:

- overall platform health
- domain pipeline health
- Kafka and streaming progress
- Spark performance
- Delta storage health
- data quality
- business freshness and SLAs

### Operational Metadata

Pipeline runs, quality outcomes, reconciliation results and alert
events are retained in governed operational metadata tables.

This allows incidents to be traced using run identifiers and supports
historical reliability analysis.

### Alerting Architecture

Alerts are generated only when an operational condition requires human
attention or automated remediation.

Atlas does not alert for every anomaly. Expected and successfully
handled conditions remain visible through logs and metrics without
creating notification noise.

Alert severity is based on business impact, recoverability and urgency.

#### Warning Alerts

Warning alerts indicate abnormal conditions that require investigation
but do not yet prevent the platform from meeting its business
commitments.

Examples include:

- rising Kafka consumer lag
- increasing invalid-record rate
- unusual duplicate percentage
- state-store growth
- delayed processing that has not yet breached the SLA
- recoverable infrastructure degradation

#### Critical Alerts

Critical alerts indicate that the platform cannot safely complete its
business objective or has already breached a committed SLA.

Examples include:

- pipeline failure
- retry exhaustion
- freshness SLA breach
- schema contract failure
- reconciliation mismatch affecting business correctness
- unavailable critical source or target
- sustained Kafka lag causing stale data
- data-quality failure that prevents Silver updates

#### No-Alert Conditions

Expected and safely handled events do not generate alerts.

Examples include:

- isolated duplicate events
- small numbers of quarantined records within threshold
- planned maintenance
- successful retries
- informational schema observations

#### Alert Payload

Every alert contains:

- severity
- environment
- domain
- pipeline
- run or query identifier
- detection time
- affected data product
- observed symptom
- likely business impact
- exception or threshold summary
- dashboard or log context
- runbook action

#### Alert Storm Prevention

Atlas applies:

- alert deduplication
- cooldown periods
- grouping of related failures
- suppression during planned maintenance
- recovery notifications

Alert lifecycle events are stored in governed operational metadata for
audit and reliability analysis.

## 12. Security, Governance and Data Protection

Atlas is designed as a governed enterprise data platform.

Security, governance and data protection are fundamental architectural
capabilities that are integrated into every layer of the platform rather
than being added after implementation.

The platform ensures that data is secure, traceable, compliant and
accessible only to authorized consumers.

---

### Objectives

Atlas governance aims to:

- protect sensitive business data
- maintain data ownership
- provide complete lineage
- support auditing
- enforce access control
- ensure regulatory compliance
- establish trusted data products

---

### Data Ownership

Every dataset within Atlas has clearly defined ownership.

Ownership includes:

- business owner
- technical owner
- source system
- downstream consumers
- SLA responsibility
- lifecycle management

Ownership ensures accountability throughout the platform.

---

### Data Classification

Atlas classifies data according to sensitivity.

Typical classifications include:

**Public**

Information that may be shared without restrictions.

Examples:

- public product catalog
- public documentation

---

**Internal**

Business information intended only for internal users.

Examples:

- operational reports
- inventory metrics
- platform monitoring

---

**Confidential**

Sensitive business information requiring controlled access.

Examples:

- customer information
- payment status
- supplier contracts
- financial metrics

---

**Restricted**

Highly sensitive information requiring strict protection.

Examples:

- passwords
- authentication secrets
- encryption keys
- access credentials
- API tokens

Restricted information must never appear in application logs or source
control.

---

### Access Control

Access follows the principle of least privilege.

Consumers receive only the permissions required to perform their role.

Typical permissions include:

- read
- write
- administer
- approve
- audit

Access policies are enforced at the platform level rather than within
individual pipelines.

---

### Secrets Management

Secrets are never committed to source control.

Examples include:

- database passwords
- API keys
- Kafka credentials
- cloud credentials
- encryption keys

Local development uses environment variables through `.env` files.

Production environments will use managed secret services.

---

### Data Lineage

Atlas records lineage throughout the platform.

Lineage includes:

- source system
- ingestion pipeline
- Bronze dataset
- Silver dataset
- Gold data product
- pipeline run identifier
- processing timestamp
- code version

Lineage enables complete traceability from business reports back to the
original operational source.

---

### Auditability

Atlas records operational metadata for every pipeline execution.

Examples include:

- pipeline runs
- configuration version
- processing duration
- validation results
- reconciliation outcomes
- alert history

Audit records support operational investigations and compliance.

---

### Data Retention

Retention policies are defined according to dataset type.

Examples include:

- Bronze retains historical source data for replay.
- Silver retains canonical business state and historical records where
  required.
- Gold retains business data products according to consumer needs.
- Operational logs and metrics follow platform retention policies.

Retention periods are documented and managed centrally.

---

### Privacy

Atlas minimizes unnecessary exposure of personally identifiable
information (PII).

The platform supports:

- data masking
- selective column access
- role-based visibility
- secure handling of sensitive attributes

Sensitive information must not be written to logs, error messages or
monitoring systems.

---

### Future Governance Roadmap

The governance architecture is designed to evolve with the platform.

Future capabilities include:

- Unity Catalog
- centralized access policies
- column-level security
- row-level security
- automated lineage
- data catalog integration
- policy enforcement
- regulatory compliance reporting

These capabilities extend the existing governance model without
requiring architectural redesign.

---

### Design Principles

Atlas governance follows these principles:

- Least privilege access
- Ownership before access
- Security by design
- Governance by default
- Complete lineage
- Auditability
- Protection of sensitive information
- Compliance through architecture

## 13. Non-Functional Requirements

Non-functional requirements define the quality attributes of the Atlas
platform.

While functional requirements describe what Atlas does,
non-functional requirements define how well the platform must perform
its responsibilities.

These requirements guide architectural decisions throughout the
platform lifecycle.

---

### Performance

Atlas must process business data within agreed freshness objectives.

Initial design targets include:

| Pipeline | Target |
|----------|---------|
| Customer CDC → Silver | ≤ 5 minutes |
| Product CDC → Silver | ≤ 10 minutes |
| Order Events → Silver | ≤ 2 minutes |
| Payment Events → Silver | ≤ 2 minutes |
| Inventory Events → Silver | ≤ 5 minutes |

Pipeline execution times are continuously monitored through platform
metrics.

---

### Scalability

Atlas is designed to scale as business volume grows.

The architecture should support:

- increasing data volumes
- increasing event throughput
- additional business domains
- additional data products
- additional platform consumers

Scaling should primarily be achieved through horizontal expansion rather
than architectural redesign.

---

### Availability

The platform should maximize availability while ensuring business
correctness.

Local development environments prioritize developer productivity.

Future production environments will define formal availability targets
and high-availability strategies.

---

### Reliability

Atlas prioritizes correctness over processing speed.

The platform must support:

- deterministic processing
- idempotent execution
- safe recovery
- replay
- reconciliation

Failures must never silently compromise canonical business state.

---

### Recoverability

Atlas must recover safely from operational failures.

Recovery mechanisms include:

- checkpoint recovery
- replay from Bronze
- historical backfills
- rebuilding Silver
- regenerating Gold

Recovery procedures must be deterministic and repeatable.

---

### Maintainability

The platform is designed for long-term evolution.

Maintainability is achieved through:

- domain ownership
- modular architecture
- reusable platform capabilities
- documented ADRs
- engineering standards
- automated testing

Architecture should favour clarity over unnecessary complexity.

---

### Observability

Every production pipeline must expose sufficient operational telemetry.

Observability includes:

- structured logs
- metrics
- dashboards
- alerts
- operational metadata

Failures should be diagnosable without modifying application code.

---

### Security

Atlas follows a security-by-design approach.

The platform must:

- protect sensitive information
- enforce least-privilege access
- separate secrets from source code
- support auditing
- preserve lineage

---

### Testability

Every platform capability should be independently testable.

Testing includes:

- unit tests
- integration tests
- end-to-end pipeline tests
- reconciliation validation
- performance testing

Testability is considered during architecture rather than added later.

---

### Extensibility

Atlas is designed to evolve without major architectural redesign.

Future platform capabilities include:

- Airflow
- dbt
- Snowflake
- Kubernetes
- Terraform
- Unity Catalog
- AI Feature Store
- Vector Databases

These technologies extend the platform rather than replace existing
architecture.

---

### Cost Efficiency

Atlas should use infrastructure efficiently while maintaining business
requirements.

Performance optimizations must be based on measured evidence rather than
assumptions.

Resource utilization, storage growth and processing costs should be
continuously monitored as the platform evolves.

### Engineering Quality

Atlas is developed using a disciplined engineering process.

Every feature follows:

- Business Requirement
- Functional Requirement
- Non-Functional Requirement
- High-Level Design
- Low-Level Design
- Implementation
- Code Review
- Testing
- Performance Review
- Production Readiness Review
- Documentation

Engineering quality is treated as a platform capability rather than an
optional development practice.

## 14. Deployment Evolution and Environment Strategy

Atlas is designed to evolve through multiple deployment stages rather
than targeting a full production cloud architecture from the beginning.

Each stage introduces new platform capabilities while preserving the
overall architectural principles and minimizing unnecessary migration.

The deployment strategy emphasizes incremental evolution, allowing the
platform to grow in complexity alongside business requirements and
engineering maturity.

---

### Stage 1 — Local Development

Purpose:

Provide a fast and productive local development environment.

Components:

- PyCharm
- Python
- Spark local[*]
- Delta Lake
- Docker Compose
- Kafka
- PostgreSQL
- Local File System

Characteristics:

- rapid debugging
- simplified infrastructure
- local experimentation
- deterministic testing

---

### Stage 2 — Object Storage

Purpose:

Introduce production-like object storage semantics.

Storage migrates from the local filesystem to MinIO while preserving the
existing application architecture.

Benefits include:

- S3-compatible APIs
- object storage semantics
- improved testing of production workflows

---

### Stage 3 — Platform Orchestration

Purpose:

Automate scheduling, dependency management and operational workflows.

Future capabilities include:

- Airflow
- workflow orchestration
- scheduled pipelines
- dependency management
- automated retries

---

### Stage 4 — Enterprise Data Platform

Purpose:

Introduce enterprise analytics and governance capabilities.

Future technologies include:

- Databricks
- Unity Catalog
- dbt
- Snowflake

These technologies extend the existing platform architecture rather than
replacing it.

---

### Stage 5 — Cloud Native Platform

Purpose:

Deploy Atlas as a scalable cloud-native enterprise platform.

Future capabilities include:

- Kubernetes
- Terraform
- cloud object storage
- managed Kafka
- managed databases
- distributed Spark clusters

---

### Migration Philosophy

Every deployment stage builds upon the previous one.

Business logic should remain independent of deployment technology.

Infrastructure changes should require minimal changes to business
transformations.

The platform architecture is designed to evolve without large-scale
rewrites.

## 15. Architecture Decision Summary

Atlas documents significant architectural decisions using Architecture
Decision Records (ADRs).

Each ADR records:

- the architectural context
- the decision
- alternatives considered
- trade-offs
- consequences
- future review triggers

The following ADRs have been accepted during the initial platform
design.

| ADR | Decision |
|------|----------|
| ADR-001 | Domain-Owned Canonical Silver |
| ADR-002 | Delta Lake as Initial Lakehouse Format |
| ADR-003 | Domain-Oriented Repository Structure |
| ADR-004 | Local Development and Runtime Architecture |

Future architectural changes must be documented through new ADRs before
implementation begins.

This ensures that important engineering decisions remain transparent,
reviewable and traceable throughout the platform lifecycle.

## 16. Risks, Assumptions and Constraints

The initial version of Atlas is intentionally built with a simplified
runtime architecture to maximize learning and developer productivity.

Several assumptions and constraints influence the current design.

### Assumptions

- Operational source systems provide reliable business events.
- CDC feeds remain consistent with authoritative snapshots.
- Business domains maintain ownership of their canonical datasets.
- Replay from Bronze is always possible.
- Future technologies will integrate with the existing architecture.

### Constraints

- Initial development occurs on a single developer workstation.
- Spark executes locally.
- Kafka uses a single broker.
- Local storage is used before migrating to object storage.
- Production cloud services are introduced incrementally.

### Risks

Potential risks include:

- Local behaviour differing from distributed production.
- Growing infrastructure complexity as new technologies are introduced.
- Schema evolution requiring careful contract management.
- Increasing operational overhead as the platform scales.
- Technical debt if architectural decisions are not continuously
  reviewed.

Atlas mitigates these risks through:

- Architecture Decision Records
- Engineering Principles
- Weekly Architecture Reviews
- Continuous refactoring
- Production Readiness Reviews

## 17. Future Roadmap

Atlas is designed as a continuously evolving enterprise platform.

Future capabilities will extend the existing architecture rather than
creating separate projects.

Planned evolution includes:

### Data Engineering

- Airflow
- dbt
- Snowflake
- Apache Iceberg
- Multi-cluster Spark

### Platform Engineering

- Kubernetes
- Terraform
- CI/CD pipelines
- Infrastructure as Code
- Automated deployments

### Governance

- Unity Catalog
- OpenLineage
- Data Catalog
- Fine-grained access control
- Policy enforcement

### Observability

- Prometheus
- Grafana
- OpenTelemetry
- Distributed tracing
- Automated alerting

### Artificial Intelligence

- Feature Store
- Vector Database
- Retrieval-Augmented Generation (RAG)
- LLM-powered data applications
- Model monitoring
- AI inference pipelines

Every future capability must integrate into Atlas through the established
architecture, engineering standards and governance model.

The platform evolves through incremental improvements while preserving
architectural consistency and long-term maintainability.