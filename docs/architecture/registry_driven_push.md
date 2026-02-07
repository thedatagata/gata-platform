# Registry-Driven Push Architecture

This architectural document outlines the Registry-Driven Push Architecture, a
contract-first design for a multi-tenant data platform. It is designed to
decouple raw data storage from business logic, ensuring that the warehouse
remains stable even as API versions or transformation requirements evolve.

## 1. Overall Architecture Overview

The platform operates on a "Thin Master Model" principle:

- **Intelligence resides in Metadata and Python**: Routing decisions and data
  contract mapping happen during onboarding, not at dbt runtime.
- **Immutable Sinks**: Master Models are multi-tenant table nodes that store
  only raw payloads and structural metadata. They contain no logic.
- **Active Staging**: Staging models act as "Pushers" that use an idempotent
  MERGE to load data into their designated sinks.
- **Dynamic Intermediate Layer**: Transformation logic (UTM rules, filters) is
  applied by joining raw data to a versioned Table-Level Logic Satellite.

## 2. Layer Responsibilities

### Phase 1: The Library Layer (Contract Definition)

- **Responsible for defining the "DNA" of the platform.** It maps every unique
  source table structure to a permanent logical destination.
- **Responsibility**: Scan supported source schemas, calculate structural
  hashes, and maintain the Master Model Registry.

### Phase 2: The Orchestration Layer (Onboarding & Routing)

- **The "brain" of the operation.** It uses the Library to "wire" a new tenant's
  data into the warehouse.
- **Responsibility**: Scaffold staging shims, provision empty Master Sinks, and
  hardcode the routing macro calls.

### Phase 3: The Staging Layer (The Pushers)

- **Active agents that normalize source data into a standard wrapper.**
- **Responsibility**: Wrap raw records in JSON, calculate the tenant's current
  schema hash, and execute the `sync_to_master_hub` macro.

### Phase 4: The Master Model Layer (Thin Multi-Tenant Sinks)

- **The definitive source of truth for raw historical data across all tenants.**
- **Responsibility**: Act as a permanent, immutable table shell for records
  sharing a common data contract.

### Phase 5: The Metadata Layer (Logic History)

- **Tracks the "intent" of transformations over time.**
- **Responsibility**: Version individual table configurations (UTMs, logic
  blocks) into unique hashes.

## 3. Detailed File Responsibilities

| File Category     | Primary File                                         | Responsibility                                                                                  |
| :---------------- | :--------------------------------------------------- | :---------------------------------------------------------------------------------------------- |
| **Library**       | `scripts/initialize_connector_library.py`            | Calculates schema hashes and populates the `connector_blueprints` registry.                     |
| **Orchestration** | `scripts/onboard_tenant.py`                          | Queries the registry, scaffolds "Thin Sink" Master Models, and generates "Push" Staging models. |
| **Orchestration** | `mock-data-engine/orchestrator.py`                   | Enforces that every tenant must have a valid mix of Ecommerce, Analytics, and Ads sources.      |
| **Push Macro**    | `macros/onboarding/sync_to_master_hub.sql`           | Performs the idempotent MERGE from Staging into the target Master Model.                        |
| **Metadata**      | `satellites/platform_sat__tenant_config_history.sql` | Flattens the manifest to generate deterministic `logic_hash` values for individual tables.      |
| **Sinks**         | `master_models/platform_mm__{id}.sql`                | Empty table shell nodes representing a specific data contract.                                  |

## 4. Order of Operations for Building from Scratch

To ensure stability and prevent "crawling" errors, the platform must be built in
this specific sequence:

1. **Initialize the Library**: Run `initialize_connector_library.py` to create
   the `connector_blueprints` table. This establishes the "Laws of Routing".
2. **Implement the Push Macro**: Create `sync_to_master_hub.sql`. Staging cannot
   exist without this macro.
3. **Refactor the Onboarding Script**: Update `onboard_tenant.py` to use the
   registry for lookups and the "Thin Sink" template for scaffolding.
4. **Provision Master Sinks**: Run the onboarding script (or a reset script) to
   create the empty multi-tenant table nodes.
5. **Generate Staging Models**: Re-run onboarding for all tenants to generate
   models with hardcoded `sync_to_master_hub` calls.
6. **Build Metadata History**: Run `platform_sat__tenant_config_history` to
   capture current logic hashes.
7. **Data Ingestion**: Run the mock engine/DLT pipelines to land raw source
   data.
8. **Execute Transformation**: Run dbt. Staging models will "push" their data
   into Master Models automatically.

## 5. Guardrails for Antigravity

- **Rule 1**: Never use `graph.nodes` or `load_relation` to automate unions or
  routing. Routing must be hardcoded in Python during onboarding.
- **Rule 2**: Master Models must remain "Thin." They should never contain
  business logic, `ref()` calls to hubs, or `UNION ALL` statements.
- **Rule 3**: Idempotency is mandatory. All data flow from Staging to Master
  Models must use the `MERGE` pattern on `tenant_slug`, `source_platform`, and
  `md5(payload)`.
- **Rule 4**: The Hub is obsolete. Do not attempt to refactor
  `hub_tenant_sources.sql`; it must be deleted to maintain the direct-push
  architecture.
