# Gata Platform: The SaaS Data Operating System

**Status:** `ALPHA`\
**Vision:** A unified, multi-tenant data platform that enables "Zero-Touch"
analytics.\
**Frontend:** [Gata Swamp (DasGata)](./apps/gata-swamp)\
**Backend:** Python + dbt + DuckDB + MotherDuck

---

## 1. The Core Vision

We are building the backend "Operating System" for the `dasgata` frontend.
Currently, `dasgata` is a demo app with hardcoded data. This repository
(`gata-platform`) transforms it into a real SaaS product where:

1. **Tenants are onboarded automatically** (Infrastructure-as-Code).
2. **Data pipelines are generated dynamically** (Software-Defined Assets).
3. **Analytics are available instantly**.

### The "Zero-Touch" Workflow

1. **User** (in `gata-swamp` UI) clicks "Connect Shopify".
2. **Backend** updates `tenants.yaml`.
3. **Ingestion** (`dlt`) pipeline runs to ingest data to MotherDuck/DuckDB.
4. **Transformation** (dbt) runs the **5-Phase Onboarding Sequence** to generate
   models.
5. **Frontend** enables analytics for Shopify data.

---

## 2. Architecture & Tech Stack

We utilize a modern, high-performance, single-node scalable stack:

| Component          | Technology                  | Role                               | Source Repo for Patterns |
| :----------------- | :-------------------------- | :--------------------------------- | :----------------------- |
| **Ingestion**      | **dlt** (Data Load Tool)    | Extract & Load, Schema Mechanics   | `mock-data-engine`       |
| **Warehouse**      | **MotherDuck** / **DuckDB** | Cloud Data Warehouse / Local Dev   | N/A                      |
| **Transformation** | **dbt Core**                | Modeling (Data Vault 2.0), Testing | `dbt-meta-config`        |
| **API**            | **FastAPI**                 | REST Interface for Frontend        | New                      |

### Conceptual Architecture: Shell & Engine

We use a **Shell & Engine** architecture to manage multi-tenancy:

- **Shells**: Tenant-specific configuration and lightweight wrappers.
- **Engines**: Centralized, robust logic (dbt macros, Python classes) that
  process data for all tenants.

### Directory Structure (The Unification)

This monorepo unifies the lifecycle of all components:

```plaintext
gata-platform/
├── .agent/                    # [Agent Intelligence] Context & Workflows for AI Engineers
├── apps/
│   └── gata-swamp/            # [Frontend] The Deno/Fresh UI (DasGata)
├── services/
│   └── mock-data-engine/      # [Ingestion] dlt pipelines and generators
├── warehouse/                 
│   └── gata_transformation/   # [Engine] The dbt Project (Data Vault 2.0)
│       ├── models/            # Hub-and-Spoke Data Vault models
│       ├── macros/            # "Code that writes code" (generate_tenant_key, engines)
│       └── seeds/             # Static reference data
├── scripts/                   # [Automation]
│   └── onboard_tenant.py      # Scaffolds dbt models
├── static/tenants/            # [Registry] JSON Semantic Definitions
├── tenants.yaml               # [Config] The Single Source of Truth
└── pyproject.toml             # Unified uv dependency management
```

---

## 3. Operational Patterns

### The 5-Phase Tenant Onboarding Lifecycle

We do not manually write SQL for every new tenant. We use a **Deterministic
Onboarding Sequence**:

- **Phase 0: The Manifest**: Define the tenant in `tenants.yaml`. This is the
  Source of Truth.
- **Phase 1: Foundation**: Reset metadata and validate the tenant against the
  platform registry.
- **Phase 2: Scaffolding**: Generate the physical directory structure and
  `_sources.yml` files.
- **Phase 3: Shim & Source**: Create `src_*.sql` models to normalize raw data
  into the platform schema.
- **Phase 4: Staging & Linkage**: Build `stg_*.sql` models and link them to
  **Master Hubs** (Data Vault).
- **Phase 5: Mart & Activation**: Materialize Datamarts.

### The Agent Intelligence Layer

This project uses an **Agent Intelligence Layer** (stored in `.agent/`) to guide
AI assistants.

- **No Guessing**: We rely on explicit schema forensics and truth gates, not
  assumptions.
- **Workflows**: Standardized procedures for scaffolding, deployment, and
  verification are codified in `.agent/workflows`.

---

## 4. Development Workflow

### Prerequisites

- `uv` (for Python dependency management)
- `deno` (for Frontend)
- `duckdb` (CLI optional, but recommended)

### Getting Started

1. **Install Dependencies:**
   ```bash
   uv sync --all-groups
   ```

2. **Start the Platform:**
   ```bash
   # (Command to start services/API would go here)
   ```

3. **Start Frontend:**
   ```bash
   cd apps/gata-swamp && deno task start
   ```

### Adding a New Connector

To add a new data source (e.g., "TikTok Ads"):

1. **Mock Generator**: Add `tiktok_ads` generator to
   `services/mock-data-engine`.
2. **Define Connector**: Add entry to the supported connectors list.
3. **Master Model**: Scaffold `platform_mm__tiktok_ads_{object}.sql` (Master
   Hub) in the warehouse.
4. **Verify**: Run `onboard_tenant.py` to ensure the new source is discovered
   and mapped.
