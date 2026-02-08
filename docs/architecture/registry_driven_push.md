# Registry-Driven Push Architecture

This architectural document outlines the Registry-Driven Push Architecture, a
contract-first design for a multi-tenant data platform. It decouples raw data
ingestion from business logic, ensuring warehouse stability through physical
relational DNA and Pydantic data contracts.

## 1. Overall Architecture Overview

The platform operates on a "Physical Truth" principle:

- **Relational DNA Discovery**: Routing is determined by calculating an MD5 hash
  of the physical column names and data types landed in the warehouse, creating
  a unique structural fingerprint for every source object.
- **Pydantic Data Contracts**: Python-native schemas serve as the source of
  truth for the landing tables. By utilizing optional fields and "frozen" types,
  the warehouse remains resilient to DuckDB/MotherDuck constraint limitations
  while enforcing relational integrity.
- **Active Staging (Pushers)**: Staging models are generated dynamically to act
  as "Pushers." They re-bundle flattened relational data into a standardized
  `raw_data_payload` JSON object for delivery to Master Models.
- **Universal Intermediate Engines**: Intermediate models dynamically union all
  Master Model sources for a specific entity (e.g., Orders) based on the
  Relational DNA registry, creating a unified star schema for the platform.
- **Boring Semantic Layer (BSL)**: A metadata-driven semantic layer is
  automatically populated from the Star Schema to provide an AI-ready interface
  for Natural Language Querying (NLQ).

## 2. Layer Responsibilities

### Phase 1: The Library Layer (DNA Definition)

Responsible for defining the "DNA" of the platform. It maps unique source
structures to permanent logical destinations.

- **Responsibility**: Scan supported source schemas, calculate structural DNA
  hashes from physical relational columns, and maintain the
  `connector_blueprints` registry.

### Phase 2: The Orchestration Layer (Onboarding & Discovery)

The "brain" of the operation. It uses the Library to "wire" a new tenant's data
into the warehouse.

- **Responsibility**: Execute data landing, discover physical relational DNA,
  and scaffold staging pushers hard-wired to the correct Master Model sinks.

### Phase 3: The Staging Layer (The Pushers)

Active agents that normalize source data into a standard wrapper.

- **Responsibility**: Re-bundle validated relational columns into a unified JSON
  payload and execute the `generate_staging_pusher` macro to move data to the
  multi-tenant sinks.

### Phase 4: The Master Model Layer (Multi-Tenant Sinks)

The definitive source of truth for raw historical data across all tenants.

- **Responsibility**: Act as an immutable table shell for records sharing a
  common data contract (e.g., `platform_mm__shopify_api_v1_orders`).

### Phase 5: The Semantic & Metadata Layer (Logic & Discovery)

Tracks technical history and exposes business logic for AI consumption.

- **Responsibility**: Version technical schema history in satellite tables and
  generate BSL manifests to support accurate NLQ across the star schema.

## 3. Detailed File Responsibilities

| File Category     | Primary File                              | Responsibility                                                                                                  |
| :---------------- | :---------------------------------------- | :-------------------------------------------------------------------------------------------------------------- |
| **Library**       | `scripts/initialize_connector_library.py` | Calculates DNA hashes from physical columns and populates the `connector_blueprints` registry.                  |
| **Orchestration** | `scripts/onboard_tenant.py`               | Lands initial data, performs DNA discovery, and scaffolds dynamic Staging models.                               |
| **Orchestration** | `mock-data-engine/orchestrator.py`        | Uses Polars for optimized extraction and enforces Pydantic schemas as stable data contracts.                    |
| **Push Macro**    | `macros/generate_staging_pusher.sql`      | Performs the idempotent bundling and push from Staging into the target Master Model.                            |
| **Metadata**      | `mock-data-engine/main.py`                | Persists dlt schema history and BSL manifests into `platform_sat__source_schema_history`.                       |
| **Semantic**      | `mock-data-engine/utils/bsl_mapper.py`    | Auto-populates dimensions and measures for Rill and the Boring Semantic Layer API.                              |
| **Contracts**     | `mock-data-engine/schemas/*.py`           | Enforces relational types (e.g., float for prices) while ensuring DuckDB compatibility through optional fields. |

## 4. Order of Operations for Building from Scratch

1. **Initialize the Library**: Run `initialize_connector_library.py` to populate
   `connector_blueprints`. This establishes the Relational DNA routing for all
   13 supported sources.
2. **Onboard Tenant**: Run `onboard_tenant.py` to land initial data and scaffold
   staging pushers. This creates models hard-coded with DNA-verified routing.
3. **Materialize Staging**: Run `dbt` to execute the pushers. Flattened source
   data is re-bundled and moved into the Master Model multi-tenant sinks.
4. **Execute Universal Engines**: Materialize intermediate models (e.g.,
   `int_unified_orders`) that drain the Master Models into unified star schema
   tables.
5. **Register Semantic Manifests**: Run `main.py` to capture the final physical
   schema, generate the BSL manifest for the star schema, and update the
   metadata satellite.

## 5. Guardrails for Stability

- **Rule 1: DNA-Based Routing**. Routing must be hard-coded in Python based on
  the physical column signature of the landed data, never automated via dynamic
  dbt lookups at runtime.
- **Rule 2: Contract Stability**. All Pydantic schema fields must remain
  `Optional` with `None` defaults to prevent NOT NULL parser errors during
  DuckDB schema evolution.
- **Rule 3: Universal Intermediate Models**. Engines should target Master Models
  by relational type (discovered via registry), ensuring a unified star schema
  regardless of whether a tenant uses Shopify, WooCommerce, or BigCommerce.
- **Rule 4: Deterministic Semantics**. The BSL must sit on the Star Schema
  (Fact/Dim tables) to ensure accurate NLQ and prevent LLM hallucinations on
  raw, uncleaned data.
