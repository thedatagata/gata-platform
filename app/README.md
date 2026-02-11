# DATA_GATA - Self-Service Analytics Platform

DATA_GATA is a sophisticated analytics dashboard that democratizes data access through two distinct
user experiences. The project combines modern web technologies with local AI capabilities to provide
both guided pivot table analysis and natural language query interfaces. Built on Deno Fresh with
Preact, the application showcases advanced feature flagging, semantic layer architecture, and
client-side AI inference.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Technical Stack](#technical-stack)
4. [Getting Started](#getting-started)
5. [Project Structure](#project-structure)
6. [LaunchDarkly Implementation](#launchdarkly-implementation)
7. [Key Features](#key-features)
8. [Development Workflow](#development-workflow)
9. [Contributing](#contributing)
10. [Troubleshooting](#troubleshooting)

---

## Project Overview

DATA_GATA provides two distinct analytics experiences designed for different user personas and skill
levels. The dual-experience architecture allows you to gradually introduce users to more
sophisticated analytics capabilities while maintaining ease of use for beginners.

### Starter Experience

The Starter experience focuses on guided analytics through interactive pivot tables powered by
WebDataRocks. Users can explore pre-aggregated session and user data without writing SQL, making
analytics accessible to non-technical team members. The interface includes natural language prompts
that translate to SQL via MotherDuck's AI capabilities, providing a gentle introduction to data
querying.

Key characteristics include:

- **1MB data limits** to ensure fast performance
- **Preset query configurations** for common analytics patterns
- **Automatic query generation** from natural language descriptions
- **Interactive pivot tables** with drill-downs, filtering, and export

### Smarter Experience

The Smarter experience introduces semantic layer querying with local AI inference through WebLLM.
This advanced interface allows users to ask questions in natural language and receive automatically
generated SQL queries, visualizations, and insights. The semantic layer abstracts complex database
schemas into user-friendly business terminology.

Key characteristics include:

- **7B parameter language model** running entirely in-browser via WebLLM
- **Semantic layer** translating business terms to database schema
- **Automatic visualizations** using Chart.js with intelligent chart detection
- **Complete data privacy** with local AI inference

### Data Model

The application works with marketing analytics data modeled after Amplitude's event tracking
structure:

**Session-Level Facts** (345K sessions):

- Temporal data (dates, timestamps, session sequences)
- Traffic attribution (sources, campaigns, referrers)
- Lifecycle stages (awareness, interest, consideration, trial, activation)
- Conversion metrics (events, revenue, engagement)

**User-Level Facts** (254K users):

- Engagement metrics (total events, active days, session counts)
- Revenue data (lifetime value, plan tiers, payment history)
- Lifecycle progression (stage reached flags, activation status)
- Cohort analysis (first event dates, retention windows)

Both tables are stored in MotherDuck cloud data warehouse and materialized locally into DuckDB WASM
for fast client-side query execution.

---

## Architecture

### System Architecture

DATA_GATA follows a three-tier architecture with clear separation between layers:

**Presentation Layer**:

- Deno Fresh routes for file-based routing
- Preact islands for interactive components with selective hydration
- Server-side rendering by default for optimal initial load performance

**Semantic Layer**:

- Business-friendly abstraction over database schemas
- Field definitions with metadata (types, categories, value ranges)
- Dimension mappings with transformations (CASE statements, formatting)
- Measure definitions with aggregations and formulas

**Data Layer**:

- MotherDuck for cloud data warehouse and persistent storage
- DuckDB WASM for client-side SQL execution
- Materialization during loading phase for optimal query performance
- Automatic sanitization for DuckDB-specific types (BigInt, Uint8Array)

### Experience Routing Flow

```
Landing Page
    ↓
LaunchDarkly Flag Evaluation (analytics-experience)
    ↓
┌───────────────┴───────────────┐
↓                               ↓
Starter Experience              Smarter Experience
(/app/dashboard/starter)        (/app/dashboard/smarter)
    ↓                               ↓
BaseDashboardWizard             SmartDashLoadingPage
    ↓                               ↓
├─ MotherDuck AI                ├─ WebLLM Initialization
├─ WebDataRocks Pivot           ├─ Semantic Layer Config
└─ 1MB Data Limit               └─ Chart.js Autovisualization
```

### Data Flow Architecture (Smarter Experience)

1. **User Input**: Natural language prompt (e.g., "show me revenue by plan tier for last 30 days")
2. **WebLLM Processing**: Model receives prompt + semantic metadata catalog
3. **Query Specification**: Model generates JSON with dimensions, measures, filters
4. **Validation**: Semantic validator checks field names against metadata
5. **SQL Translation**: Semantic objects module converts to executable SQL
6. **Query Execution**: DuckDB WASM executes against local materialized tables
7. **Result Analysis**: Query response analyzer recommends visualization types
8. **Chart Generation**: Chart generator creates Chart.js configuration
9. **Rendering**: AutoChart island displays interactive visualization

### Semantic Layer Architecture

The semantic layer uses a three-tier structure:

**Fields (Raw Columns)**:

```json
{
  "plan_tier": {
    "md_data_type": "VARCHAR",
    "ingest_data_type": "string",
    "data_type_category": "categorical",
    "members": ["starter", "smarter", "premium", null]
  }
}
```

**Dimensions (User-Friendly Groupings)**:

```json
{
  "plan_tier": {
    "alias_name": "Current Plan Tier",
    "transformation": "CASE WHEN plan_tier IS NULL THEN 'no plan' ELSE plan_tier END"
  }
}
```

**Measures (Business Metrics)**:

```json
{
  "session_revenue": {
    "aggregations": [{
      "sum": {
        "alias": "Total Revenue",
        "format": "currency",
        "decimals": 2
      }
    }]
  }
}
```

This structure enables natural language queries while maintaining SQL accuracy.

---

## Technical Stack

### Frontend Framework

**Deno Fresh 1.7.3**:

- Modern web framework with file-based routing
- Island architecture for selective hydration (only interactive components)
- Server-side rendering by default for optimal performance
- TypeScript native with no build step required

**Preact 10.22.0**:

- React-compatible API with 3KB bundle size
- Virtual DOM for efficient updates
- Hooks support for component logic
- Compatible with React ecosystem tools

### Data Layer

**DuckDB WASM 0.7.0**:

- In-browser SQL query engine via WebAssembly
- Full SQL analytics capabilities without backend
- ~10MB initial load, cached for subsequent sessions
- Handles complex queries (joins, window functions, CTEs)

**MotherDuck**:

- Cloud data warehouse built on DuckDB
- Persistent storage for analytics tables
- Streaming materialization to local instances
- Handles authentication and data updates

### AI and Machine Learning

**WebLLM 0.2.72**:

- Browser-based LLM inference via WebGPU
- No external API calls, complete data privacy
- Model caching in IndexedDB (~4.5GB)
- Supports multiple model architectures

**DeepSeek-R1-Distill-Qwen-7B**:

- 7 billion parameter model optimized for code generation
- Quantized to 4-bit precision (q4f16_1) for efficiency
- Strong performance on structured output (JSON, SQL)
- ~4GB memory footprint during inference

### Visualization Libraries

**Chart.js 4.4.0**:

- Canvas-based rendering for smooth animations
- Responsive design with touch support
- Extensive plugin ecosystem
- Tree-shakeable modular architecture

**chartjs-chart-funnel 4.1.2**:

- Funnel chart extension for Chart.js
- Lifecycle stage visualization
- Conversion rate calculations
- Customizable stage ordering

**WebDataRocks 1.3.4**:

- Interactive pivot table library
- Drag-and-drop interface for exploration
- Export to Excel, CSV, PDF
- Calculated fields and custom aggregations

### Feature Flagging

**LaunchDarkly JS Client SDK 3.3.0**:

- Real-time flag evaluation with streaming
- Multi-context targeting (user, device, session)
- Percentage rollouts for gradual releases
- Event tracking for experimentation

### The Dual-Experience Model

DATA_GATA implements a progressive disclosure pattern through two separate user experiences. Users
begin at a landing page where a LaunchDarkly feature flag evaluation determines which experience
they should access based on their plan tier, usage patterns, and experimentation groups.

The **Starter experience** focuses on simplicity and speed. It provides guided analytics through
interactive pivot tables powered by WebDataRocks, combined with MotherDuck's AI-powered SQL
generation. This experience targets users who want to explore data without technical SQL knowledge,
offering a 1MB data limit to ensure consistently fast performance. The implementation resides
primarily in `/islands/dashboard/starter_dashboard/` and uses `/utils/starter/` for query building
and slice generation.

The **Smarter experience** introduces sophisticated semantic layer querying with local AI inference.
Running a 7B parameter language model entirely in the browser through WebLLM, this experience allows
users to ask questions in natural language and receive automatically generated visualizations. The
semantic layer translates business-friendly terminology into precise SQL queries, while chart
detection algorithms select appropriate visualizations based on query structure. This implementation
spans `/islands/dashboard/smarter_dashboard/` for UI components and `/utils/smarter/` for semantic
layer logic, query generation, and AI model integration.

The separation between these experiences is not merely cosmetic—they represent fundamentally
different architectural approaches to the same problem of making data accessible. The Starter
experience prioritizes immediate usability with minimal technical overhead, while the Smarter
experience embraces complexity to enable more powerful querying capabilities.

### Three-Tier Architecture

Regardless of which experience a user accesses, the application follows a consistent three-tier
architecture:

The **presentation layer** handles all user interaction and rendering. Built on Deno Fresh with
Preact, it employs an islands architecture where most content renders server-side for fast initial
page loads, while interactive components (islands) hydrate selectively on the client. This approach
minimizes JavaScript payload while maintaining rich interactivity where needed. The routing
structure in `/routes/` defines page templates, while `/islands/` contains interactive components
that require client-side JavaScript.

The **semantic layer** serves as a translation mechanism between user-friendly business terminology
and the underlying database schema. Located in `/utils/smarter/dashboard_utils/semantic-config.ts`
and related files, this layer defines fields (raw database columns), dimensions (business-friendly
groupings with optional transformations), and measures (aggregations and formulas for business
metrics). The semantic layer's metadata files in `/static/smarter/` provide the foundation for both
WebLLM query generation and manual query validation.

The **data layer** manages all interactions with the actual analytics data. MotherDuck serves as the
cloud data warehouse, hosting the persistent `amplitude.sessions_pivot_src` and
`amplitude.users_pivot_src` tables. During the loading phase, these tables materialize into local
DuckDB WASM instances running entirely in the browser. This materialization strategy enables fast,
local SQL query execution without network latency, while maintaining a source of truth in the cloud
for data updates and persistence.

## Code Organization and File Structure

### Routing and Entry Points

The application's routing structure lives in `/routes/` and follows Fresh's file-based convention.
The main entry points are:

`/routes/index.tsx` serves the landing page with the hero section and plan selection. When users
click "Get Started," they proceed to plan selection, which triggers the experience routing logic.

`/routes/app/dashboard.tsx` contains the critical routing decision point. This file initializes the
LaunchDarkly client with user context, evaluates the `analytics-experience` flag, and redirects
users to either `/app/dashboard/starter` or `/app/dashboard/smarter` based on the flag value. The
implementation builds a user context object containing email, plan tier, and usage metrics, then
passes this to LaunchDarkly for targeting evaluation. This routing file represents the gateway that
determines which entire application flow a user will experience.

`/routes/app/dashboard/starter.tsx` renders the Starter experience entry point, loading the
`BaseDashboardWizard` island component that orchestrates the pivot table workflow.

`/routes/app/dashboard/smarter.tsx` renders the Smarter experience entry point, loading the
`SmartDashLoadingPage` island that handles WebLLM initialization, semantic table materialization,
and dashboard rendering.

### Islands Architecture

Fresh's islands architecture plays a crucial role in DATA_GATA's performance profile. Most page
content renders on the server and ships as static HTML, while specific interactive regions "hydrate"
as islands with client-side JavaScript. This selective hydration keeps initial page load fast while
enabling rich interactivity where needed.

The `/islands/` directory contains all interactive components:

`/islands/onboarding/DashboardRouter.tsx` manages the initial experience selection and navigation
flow. It renders the plan selector and handles the transition from landing page to the appropriate
dashboard experience.

`/islands/onboarding/PlanSelection.tsx` provides the UI for choosing between Starter and Smarter
plans, tracking the selection event through LaunchDarkly for funnel analysis.

`/islands/app_utils/SessionTracker.tsx` handles session lifecycle tracking, firing LaunchDarkly
events when users start and end sessions, and tracking page views for analytics purposes.

### Starter Experience Implementation

The Starter experience centers on `/islands/dashboard/starter_dashboard/`, where several key
components work together:

`BaseDashboardWizard.tsx` serves as the main orchestrator for the pivot table workflow. It
implements a four-step wizard: configure (select data source), query (generate SQL via natural
language or manual input), validate (preview results), and visualize (render interactive pivot
table). The component integrates with MotherDuck's `prompt_sql()` function for AI-powered query
generation and manages the state transitions between wizard steps.

`WebDataRocksPivot.tsx` wraps the WebDataRocks library, handling data type conversions from DuckDB's
native types (BigInt, Uint8Array) to formats WebDataRocks can visualize. It also implements the
slice configuration system that determines how data pivots—which fields appear as rows, columns, and
measures. The component includes timeout detection for the 1MB data limit, triggering upgrade
prompts when users exceed capacity.

`FieldCatalog.tsx` displays the complete metadata for available fields in the selected table,
showing data types, cardinality, null percentages, and sample values. This catalog helps users
understand what data is available when crafting their queries, whether through natural language
prompts or manual SQL.

The utility functions supporting the Starter experience live in `/utils/starter/`:

`pivot-query-builder.ts` extracts columns from SQL queries, validates them against metadata, and
generates WebDataRocks slice configurations. It also handles hierarchy completion—when a user
includes a child dimension like `traffic_source` without its parent `traffic_source_type`, the
builder automatically adds the parent to maintain hierarchy integrity.

`slice-object-generator.ts` contains the logic for categorizing query columns into WebDataRocks
slice components. It examines metadata to determine which fields should become rows (dimensions),
columns (sequences), or measures (aggregations), and configures appropriate aggregation functions
based on field types.

### Smarter Experience Implementation

The Smarter experience involves considerably more complexity due to its semantic layer, local AI
inference, and automatic visualization capabilities. The implementation spans several directories:

`/islands/dashboard/smarter_dashboard/SmartDashLoadingPage.tsx` manages the initialization sequence
that occurs when users first access the Smarter experience. This component coordinates three
critical setup tasks: establishing the MotherDuck connection, materializing tables into local DuckDB
WASM instances, and loading the WebLLM model into browser memory. Progress tracking provides user
feedback during what can be a 10-30 second initialization depending on whether cached data exists.

The dashboard landing views live in `/islands/dashboard/smarter_dashboard/dashboard_landing_view/`:

`LandingOverview.tsx` presents the main dashboard view after initialization, displaying key
performance indicators for both sessions and users. It includes an AI analyst feature (gated by the
`smarter-ai-analyst-access` flag) that generates insights about trends and patterns in the data.
Users can drill into detailed views for sessions or users from this overview.

`SessionDetailsDashboard.tsx` and `UserDetailsDashboard.tsx` provide detailed analytics for their
respective data models. These components render pre-built queries from
`/utils/smarter/overview_dashboards/semantic-dashboard-queries.ts` to show lifecycle distributions,
revenue breakdowns, and cohort analyses. They also include the semantic query interface where users
can type natural language questions that WebLLM translates to SQL.

The semantic query interface is implemented in
`/islands/dashboard/smarter_dashboard/semantic_dashboard/`:

`AutoVisualizationExperience.tsx` orchestrates the complete natural language to visualization
pipeline. Users enter a question in natural language, which the component sends to the WebLLM
handler for query generation. After validation and SQL translation through the semantic layer,
DuckDB executes the query. The response analyzer then recommends visualization types based on query
structure, and the chart generator creates Chart.js configurations for rendering.

### Semantic Layer Implementation

The semantic layer represents one of the most sophisticated aspects of DATA_GATA's architecture. It
provides a translation mechanism that allows users to think in business terms while maintaining SQL
precision.

The foundation lives in `/static/smarter/`, where JSON metadata files define the semantic layer:

`semantic-sessions-metadata.json` contains complete metadata for the sessions table, defining 41
fields with their data types, categories, and value ranges. It maps these fields to 22 dimensions
(business-friendly names with optional transformations) and 26 measures (aggregation definitions
with formatting specifications). For example, the raw `plan_tier` field maps to a "Current Plan
Tier" dimension with a transformation that handles NULL values, while the `session_revenue` field
maps to a "Total Revenue" measure with sum aggregation and currency formatting.

`semantic-users-metadata.json` follows the same structure for user-level analytics, defining 41
fields mapping to 22 dimensions and 26 measures. This includes lifecycle stage dimensions,
engagement metrics, and revenue measures specific to user-level analysis.

The semantic layer logic resides in `/utils/smarter/dashboard_utils/`:

`semantic-config.ts` loads the metadata JSON files and provides accessor functions that other
components use to retrieve dimension definitions, measure specifications, and field metadata. It
also generates the prompt that WebLLM receives, which includes a complete catalog of available
dimensions and measures with their business-friendly names.

`semantic-objects.ts` implements the `SemanticReportObj` class, which translates semantic query
specifications into executable SQL. Given a query with dimension names, measure aliases, and filters
expressed in business terms, this class maps them back to their underlying field names, applies
transformations, constructs aggregation logic, and generates valid DuckDB SQL. It handles special
cases like the `_count` pseudo-measure for `COUNT(*)` and sanitizes DuckDB-specific types (BigInt,
Uint8Array) in query results.

`semantic-query-validator.ts` validates user-generated SQL by extracting column references, checking
them against metadata, and generating helpful correction prompts when invalid fields are used. This
validator supports the WebLLM retry loop—when the AI model generates SQL with incorrect field names,
the validator identifies the issue and suggests alternatives, which the model uses to generate a
corrected query.

### WebLLM Integration

The WebLLM integration enables completely private, client-side AI inference for natural language
query generation. The implementation lives in `/utils/smarter/autovisualization_dashboard/`:

`webllm-handler.ts` manages the WebLLM lifecycle, including model loading, prompt construction, and
query generation. It supports multiple model tiers (3B, 7B parameters) controlled by the
`smarter-model-tier` LaunchDarkly flag. When users enter natural language queries, this handler
constructs a prompt containing the semantic layer catalog, sends it to the local language model,
parses the JSON response into a query specification, validates it against metadata, and handles
retry loops if validation fails.

The model selection represents a significant architectural decision. Initially, the implementation
used Qwen2.5-Coder models for their coding capabilities, but experimentation revealed that
DeepSeek-R1-Distill models provided better semantic reasoning for translating business questions to
SQL. The current implementation defaults to DeepSeek-R1-Distill-Qwen-7B, which offers strong
performance while remaining practical for browser-based inference.

Model caching through IndexedDB means that after the initial ~4.5GB download, subsequent sessions
load the model from local storage, reducing startup time from minutes to seconds. The loading page
provides progress feedback during this initialization.

### Visualization System

DATA_GATA's visualization system has undergone the most significant evolution throughout the
project. The current implementation uses Chart.js, but this represents the third major attempt at
finding the right charting solution.

`/islands/charts/AutoChart.tsx` implements the primary visualization component for the Smarter
experience. It receives Chart.js configuration objects from the chart generator and renders
interactive canvas-based charts. The implementation handles multiple chart types (line, bar, area,
stacked bar) and provides consistent styling, hover interactions, and legend positioning.

`/islands/charts/FunnelChart.tsx` provides specialized funnel visualizations using the
`chartjs-chart-funnel` plugin. Funnel charts require special handling because they represent
sequential conversion flows, which don't fit standard bar or line chart paradigms.

The chart generation logic resides in `/utils/smarter/autovisualization_dashboard/`:

`chart-generator.ts` creates Chart.js configuration objects from query results and query
specifications. It analyzes the dimensions and measures in the query, selects appropriate chart
types, configures axes, applies formatting (currency, percentages), and generates title text.

`auto-chart-detector.ts` implements rule-based chart type detection. When users ask questions in
natural language, this detector examines the resulting query structure to recommend visualization
types. If a query includes a time dimension and a single measure, it suggests a line chart. Two
dimensions with a measure suggest a heatmap. Lifecycle stages suggest a funnel chart. This
rule-based approach proves more reliable than attempting AI-powered chart selection.

### LaunchDarkly Integration

Feature flagging through LaunchDarkly enables progressive rollouts, A/B testing, and controlled
feature access. The integration code lives in `/utils/launchdarkly/`:

`client.ts` initializes the LaunchDarkly JavaScript SDK with streaming enabled and localStorage
bootstrap for fast subsequent loads. It provides flag evaluation functions that components call to
check feature access.

`context-builder.ts` constructs the user context object that LaunchDarkly uses for targeting. This
includes user identification (email hash), plan tier, query execution counts, and custom attributes
like SQL expertise level.

`events.ts` defines event tracking functions organized into six categories: view events (page loads,
modal views), interaction events (clicks, form submissions), performance events (query latency,
model inference time), operation events (data operations, cache actions), error events (failures
with context), and session events (start, end, duration). Each event includes standard metadata
(timestamp, user context, plan tier) plus event-specific properties.

The implementation plan in `IMPLEMENTATION_PLAN.md` documents the phased rollout approach, with
Stage 1 (foundation and routing) complete, Stage 2 (Starter features) complete, Stage 3 (Smarter
features) recently completed, and Stage 4 (experiments and analytics) in progress.

## Data Layer and Database Architecture

### MotherDuck as Cloud Warehouse

MotherDuck serves as DATA_GATA's persistent data warehouse, hosting the analytics tables in the
cloud. The tables follow an Amplitude-inspired event tracking model:

`amplitude.sessions_pivot_src` contains 345,538 session-level records with 32 columns covering
session identification, temporal data, traffic attribution, lifecycle stages, conversion events, and
revenue metrics. This table is already denormalized and pre-aggregated for pivot table analysis,
meaning it requires no complex joins or transformations for most queries.

`amplitude.users_pivot_src` contains 254,384 user-level records with 41 columns tracking user
engagement, lifecycle progression, revenue, and attribution. The table includes both aggregate
metrics (total events, lifetime revenue) and time-windowed metrics (active in last 7 days, events in
last 30 days).

The MotherDuck connection configuration resides in `/utils/services/motherduck-client.ts`, which
handles authentication via access token and manages the connection lifecycle. The client exposes a
simple `evaluateQuery()` interface that components use to execute SQL and receive result sets.

### DuckDB WASM for Local Execution

While MotherDuck provides the persistent data store, actual query execution happens locally in the
browser through DuckDB WASM. This architecture delivers several critical benefits: zero network
latency for queries, complete data privacy (queries never leave the browser), and the ability to
perform complex analytics on larger datasets without backend infrastructure.

The materialization process occurs during `SmartDashLoadingPage` initialization. The component
executes `CREATE TABLE IF NOT EXISTS` statements that copy data from MotherDuck's cloud tables into
local DuckDB WASM instances. These local tables are named `session_facts` and `users_dim` (distinct
from their cloud counterparts to avoid naming conflicts). The semantic metadata files reference
these local table names, ensuring query generation targets the correct tables.

DuckDB WASM introduces specific data type considerations. The `BIGINT` type maps to JavaScript's
BigInt rather than Number, requiring explicit conversion before passing data to visualization
libraries. Similarly, certain binary column types return Uint8Array, which must be sanitized. The
`sanitizeQueryData()` function in multiple components handles these conversions, ensuring downstream
code receives JavaScript-native types.

### Metadata Management

Beyond the semantic metadata files, DATA_GATA maintains separate metadata for the Starter
experience's WebDataRocks integration. These files live in `/static/starter/`:

`users-pivot-metadata.json` defines the type information, hierarchy relationships, and field
categorizations for the users table. It includes the FirstTouch and LastTouch UTM hierarchies
(campaign → medium → source) that WebDataRocks uses for drill-down functionality.

`sessions_pivot_src_metadata.json` provides equivalent metadata for session-level pivot tables,
including the Traffic hierarchy (source type → source) and Lifecycle hierarchy (stage → funnel
step).

The metadata store utility at `/utils/services/metadata-store.ts` provides programmatic access to
this metadata, supporting the field catalog and query validation features.

## Technology Selection and Evolution

### Framework Choice: Deno Fresh with Preact

DATA_GATA builds on Deno Fresh, chosen for its zero-configuration TypeScript support, file-based
routing, and islands architecture. Unlike traditional React frameworks that ship entire component
trees to the client, Fresh renders most content server-side and selectively hydrates interactive
regions. This approach minimizes JavaScript payload while maintaining full interactivity where
needed.

The choice of Preact over React reduces bundle size significantly (3KB vs 45KB) while maintaining
API compatibility. Since DATA_GATA doesn't use React-specific features and prioritizes fast load
times, Preact's performance profile makes it the better choice.

Fresh's lack of a build step during development proved particularly valuable during rapid iteration.
Changes to routes or components reflect immediately without compilation wait times. The production
build step remains simple and fast, generating optimized bundles without complex webpack
configurations.

### Visualization Library Journey

The visualization system underwent three complete reimplementations before settling on the current
Chart.js solution. This evolution captures important lessons about library compatibility and
architectural constraints.

**First Attempt: Observable Plot** was chosen for its simple API and powerful default
visualizations. However, Observable Plot lacks the granular control needed for custom chart
interactions, and its D3-based approach required including the entire D3 library for basic
functionality. More critically, Observable Plot expects data in specific formats that conflicted
with DuckDB's output structure, particularly around date handling and numeric types. After
implementing the BaseDashboard with Observable Plot, performance issues with large datasets and
limited chart type support led to reconsidering the choice.

**Second Attempt: Recharts** seemed ideal as a React-native charting library with declarative APIs
and excellent TypeScript support. However, Recharts depends heavily on React-specific APIs that
Preact's compatibility layer doesn't fully replicate. Components like `ResponsiveContainer` and
chart composition patterns rely on `React.Children` APIs and context mechanisms that behave
differently in Preact. After extensive debugging attempts, it became clear that Recharts' deep React
coupling made it fundamentally incompatible with Preact, despite Preact claiming React
compatibility.

**Third Attempt: WebDataRocks** for the Starter experience proved successful because it's a
standalone JavaScript library with no framework dependencies. However, it introduced its own
complexity around slice configuration and data format requirements. WebDataRocks expects data with
metadata rows describing types, which required custom preprocessing logic. The library works well
for its intended use case (pivot tables) but revealed limitations when attempting programmatic chart
generation.

**Current Solution: Chart.js** provides the right balance of capabilities and compatibility. Being
framework-agnostic (pure JavaScript with canvas rendering), it works identically in any framework
environment. The plugin architecture supports extensions like funnel charts. The configuration
object approach allows complete programmatic control over visualizations. Most importantly, Chart.js
handles DuckDB's data types gracefully once proper sanitization occurs.

The lesson learned: when building on non-standard frameworks like Preact, choose libraries that
either explicitly support your framework or maintain complete framework independence. React-specific
libraries will cause subtle, hard-to-debug issues even when they seem to work initially.

### WebLLM for Local AI Inference

The decision to use WebLLM for local AI inference rather than calling external APIs (OpenAI,
Anthropic) reflects DATA_GATA's privacy-first philosophy. Running language models entirely in the
browser ensures that user data and queries never leave the client, providing complete data privacy
without requiring trust in third-party providers.

WebLLM leverages WebGPU for hardware acceleration when available, falling back to WebGL for broader
compatibility. The 7B parameter models deliver reasonable query generation quality while remaining
practical for browser deployment. Model caching in IndexedDB means the significant download only
occurs once, with subsequent sessions loading from local storage.

The trade-off is initialization time and memory consumption. Loading a 4.5GB model takes minutes on
first access and requires substantial RAM. However, for users prioritizing data privacy, this
trade-off proves acceptable. The implementation includes careful memory management and cleanup to
avoid browser crashes.

Model selection went through its own evolution. Initial experiments with Llama-3.2-3B showed
adequate performance but struggled with field name accuracy. Qwen2.5-Coder-3B and 7B models improved
coding-style SQL generation but sometimes misunderstood business terminology. DeepSeek-R1-Distill
models, despite being newer and less widely adopted, demonstrated superior semantic reasoning—the
ability to correctly map business questions to appropriate dimensions and measures. This finding led
to standardizing on DeepSeek-R1-Distill-Qwen-7B as the default model.

### MotherDuck over BigQuery/Snowflake

MotherDuck serves as DATA_GATA's cloud data warehouse, chosen over alternatives like BigQuery,
Snowflake, or traditional Postgres for several reasons. Built on DuckDB, MotherDuck provides a
unique capability: seamless materialization from cloud to local browser instances. Because both the
cloud warehouse and browser analytics engine use DuckDB, data moves between them with perfect
fidelity—no type conversion, no schema translation, no impedance mismatch.

MotherDuck also provides built-in AI query generation through `prompt_sql()`, which powers the
Starter experience's natural language interface. While this AI isn't as powerful as dedicated LLM
APIs, it's sufficient for guided query generation and introduces zero latency since it runs within
the data warehouse.

The cloud-to-browser architecture enabled by MotherDuck + DuckDB WASM allows DATA_GATA to perform
sophisticated analytics entirely client-side after initial data load. This wouldn't be practical
with traditional warehouses that require round-trip queries for every interaction.

### LaunchDarkly for Feature Management

LaunchDarkly provides the feature flagging infrastructure that enables DATA_GATA's progressive
rollout strategy and experimentation capabilities. The choice of LaunchDarkly over alternatives like
Split, Optimizely, or homegrown solutions reflects several priorities:

First, LaunchDarkly's targeting engine allows complex user segmentation based on multiple attributes
simultaneously. DATA_GATA can target users based on plan tier, usage patterns, SQL expertise level,
and membership in experiment groups—all expressed through simple boolean logic in the dashboard.

Second, the real-time streaming capability means flag changes propagate to active users immediately
without requiring page refreshes. This enables dynamic experiments and instant feature rollouts
without redeployment.

Third, LaunchDarkly's event tracking integrations allow using flags as both feature gates and
analytics instrumentation. Every flag evaluation generates an event that can trigger analytics
pipelines, providing insight into feature adoption and user behavior.

The implementation strategy separates routing flags (`analytics-experience`) from feature flags
(`smarter-model-tier`, `query-validation-strategy`) to enable independent control of major
experience paths versus specific feature configurations. This separation prevents unintended side
effects where changing a feature flag accidentally redirects users to different experiences.

## Lessons Learned and Technical Debt

### Semantic Layer Complexity

The semantic layer delivers significant value by abstracting database complexity, but it introduces
its own maintenance burden. Each new dimension or measure requires updating metadata files, adding
field definitions, configuring transformations, and testing query generation. This overhead means
that adding new analytics capabilities requires more work than simply writing SQL.

The validation and retry loop for WebLLM-generated queries addresses the AI's occasional field
naming errors, but it adds latency and complexity. Future improvements might cache common query
patterns or implement better prompt engineering to reduce retry frequency.

### DuckDB Type Handling

DuckDB's type system doesn't perfectly align with JavaScript's, requiring sanitization layers
throughout the codebase. The BigInt and Uint8Array conversions are necessary but scattered across
multiple files, creating maintenance risk. Centralizing this sanitization logic would improve
reliability.

### WebDataRocks Integration

The Starter experience's WebDataRocks integration works well but involves unintuitive configuration
patterns. Slice objects require precise structure, and debugging issues requires diving into
WebDataRocks internals. The library's documentation assumes familiarity with pivot table concepts
that many developers lack.

### Model Initialization Overhead

WebLLM's initialization time remains the biggest UX challenge in the Smarter experience. While model
caching helps subsequent sessions, the first-time experience involves a multi-minute wait. Exploring
smaller models, progressive loading strategies, or precomputed query templates might improve this
experience.

### LaunchDarkly Event Volume

The comprehensive event tracking strategy generates significant event volume, which could impact
LaunchDarkly costs at scale. The current implementation tracks every query execution, visualization
render, and user interaction. Production deployment might require sampling or filtering less
critical events.

## Future Architectural Considerations

### Incremental Materialization

Currently, the Smarter experience materializes complete tables during initialization, which works
for the current data size (345K sessions, 254K users) but won't scale indefinitely. Implementing
incremental materialization—downloading only recent data or data matching user context—could improve
initialization time and reduce memory consumption.

### Streaming Query Results

DuckDB WASM supports streaming query results, but the current implementation materializes complete
result sets before passing them to visualizations. For large result sets, streaming could enable
progressive visualization updates and reduce memory pressure.

### Advanced Semantic Layer Features

The semantic layer could evolve to support more sophisticated features: query hints for common
patterns, automatic index suggestions based on query frequency, or learned transformations from user
feedback. These enhancements would require significant infrastructure investment but could
dramatically improve query quality.

### Alternative Model Architectures

Newer model architectures like quantized Llama 3 or specialized code models might provide better
query generation quality or faster inference. The WebLLM integration abstracts model selection
sufficiently that swapping models should be straightforward, enabling experimentation with emerging
architectures.

### Progressive Web App Capabilities

DATA_GATA could evolve into a full progressive web app with offline capabilities. Since the semantic
layer, model, and data already live client-side, adding service worker caching and IndexedDB
persistence would enable completely offline analytics sessions. Users could load data while online,
then continue analysis offline, with changes syncing when connectivity returns.

---

This architecture overview captures DATA_GATA's current state while acknowledging the evolution that
led here. The dual-experience model, semantic layer architecture, and local AI inference represent
intentional design decisions that prioritize usability, privacy, and performance. Understanding
these architectural principles helps explain not just how the system works, but why it works this
way, and what trade-offs were accepted to achieve current capabilities.

## Getting Started

### Prerequisites

**Required Software**:

- **Deno 1.40+**: Install from [deno.land](https://deno.land)
- **Git**: For repository cloning
- **Modern Browser**: Chrome 113+, Edge 113+, or Safari 17+ (WebGPU support)

**Required Accounts**:

- **MotherDuck**: Sign up at [motherduck.com](https://motherduck.com) for API token
- **LaunchDarkly**: Sign up at [launchdarkly.com](https://launchdarkly.com) for SDK keys

**Browser Requirements**:

- WebGPU support for WebLLM (Smarter experience)
- 8GB+ RAM recommended for 7B model inference
- Modern JavaScript (ES2022) support
- IndexedDB for model caching

### Installation

Clone the repository:

```bash
git clone https://github.com/yourusername/dasgata.git
cd dasgata
```

Copy environment template and configure credentials:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
MOTHERDUCK_TOKEN=your_motherduck_token_here
LAUNCHDARKLY_CLIENT_ID=your_client_side_sdk_key
LAUNCHDARKLY_SERVER_KEY=your_server_side_sdk_key
```

Start the development server:

```bash
deno task start
```

The application will be available at `http://localhost:8000`.

### First-Time Setup

**Landing Page**:

1. Navigate to `http://localhost:8000`
2. Click "Get Started" to begin onboarding
3. LaunchDarkly evaluates your context and routes you to the appropriate experience

**Testing Experiences Directly**:

- Starter: `http://localhost:8000/app/dashboard/starter`
- Smarter: `http://localhost:8000/app/dashboard/smarter`

**WebLLM Initial Load** (Smarter experience only):

- First load downloads ~4.5GB model (3-5 minutes on fast connection)
- Model caches in IndexedDB for subsequent sessions
- Progress indicator shows download and compilation status
- Once loaded, subsequent sessions start in <10 seconds

### LaunchDarkly Configuration

Create these feature flags in your LaunchDarkly project:

**Flag 1: analytics-experience**

- Type: String (multivariate)
- Variations: `"starter"`, `"smarter"`
- Default: `"starter"`
- Targeting: Email attribute = your test email → serve `"smarter"`

**Flag 2: starter-ai-query-access**

- Type: Boolean
- Default: `false`
- Purpose: Gate AI SQL generation in Starter experience

**Flag 3: starter-data-limit**

- Type: Number
- Variations: `1048576` (1MB), `5242880` (5MB), `10485760` (10MB)
- Default: `1048576`
- Purpose: Control WebDataRocks data size limit

**Flag 4: smarter-model-tier**

- Type: String
- Variations: `"3b"`, `"7b"`
- Default: `"7b"`
- Purpose: Control WebLLM model selection

**Flag 5: query-validation-strategy**

- Type: String
- Variations: `"strict"`, `"fast"`, `"adaptive"`
- Default: `"adaptive"`
- Purpose: Control SQL validation behavior

**Flag 6: smarter-query-persistence**

- Type: Boolean
- Default: `false`
- Purpose: Enable IndexedDB query storage

**Flag 7: smarter-ai-analyst-access**

- Type: Boolean
- Default: `false`
- Purpose: Gate AI insights and recommendations

---

## Project Structure

```
dasgata/
├── routes/                    # File-based routing
│   ├── index.tsx             # Landing page
│   ├── _app.tsx              # Application shell
│   ├── app/
│   │   └── dashboard/
│   │       ├── [experience].tsx  # Route parameter handler
│   │       ├── starter.tsx   # Starter experience route
│   │       └── smarter.tsx   # Smarter experience route
│   └── api/
│       ├── motherduck-token  # Token proxy endpoint
│       └── query             # Semantic query API
│
├── islands/                   # Interactive client components
│   ├── app_utils/
│   │   ├── SessionTracker.tsx
│   │   ├── PlanSelection.tsx
│   │   └── DashboardRouter.tsx
│   ├── starter_dashboard/
│   │   ├── BaseDashboardWizard.tsx
│   │   └── WebDataRocksPivot.tsx
│   ├── smarter_dashboard/
│   │   ├── SmartDashLoadingPage.tsx
│   │   ├── dashboard_landing_view/
│   │   │   ├── LandingOverview.tsx
│   │   │   ├── SessionDetailsDashboard.tsx
│   │   │   └── UserDetailsDashboard.tsx
│   │   └── semantic_dashboard/
│   │       ├── AutoVisualizationExperience.tsx
│   │       └── SavedQueries.tsx
│   └── charts/
│       ├── AutoChart.tsx
│       └── FunnelChart.tsx
│
├── components/                # Server-rendered components
│   ├── charts/
│   │   ├── FreshChartsWrapper.tsx
│   │   └── KPICard.tsx
│   ├── UpgradeModal.tsx
│   ├── Nav.tsx
│   ├── GataFooter.tsx
│   └── Hero.tsx
│
├── utils/                     # Utility modules
│   ├── launchdarkly/         # Feature flag utilities
│   │   ├── client.ts         # Client SDK init
│   │   ├── context-builder.ts
│   │   ├── events.ts         # 6-event tracking model
│   │   └── server.ts         # Server SDK
│   ├── smarter/              # Semantic layer utilities
│   │   ├── semantic-config.ts
│   │   ├── semantic-amplitude.ts
│   │   ├── webllm-handler.ts
│   │   ├── chart-generator.ts
│   │   ├── dashboard_utils/
│   │   │   ├── semantic-query-validator.ts
│   │   │   └── query-response-analyzer.ts
│   │   └── overview_dashboards/
│   │       ├── semantic-dashboard-queries.ts
│   │       └── kpi-queries.ts
│   ├── starter/              # Pivot table utilities
│   │   ├── pivot-query-builder.ts
│   │   ├── slice-object-generator.ts
│   │   └── query-persistence.ts
│   └── services/
│       └── motherduck-client.ts
│
├── static/                    # Static assets
│   ├── smarter/              # Semantic layer configs
│   │   ├── semantic-sessions-metadata.json
│   │   ├── semantic-users-metadata.json
│   │   └── sw.js             # Service worker for WebLLM
│   └── starter_utils/        # Pivot table configs
│       ├── webdatarocks.js
│       ├── webdatarocks.css
│       ├── users-pivot-metadata.json
│       └── sessions_pivot_src_metadata.json
│
├── deno.json                  # Deno configuration
├── fresh.config.ts           # Fresh framework config
├── .env                      # Environment variables (gitignored)
├── .env.example              # Environment template
├── IMPLEMENTATION_PLAN.md    # LaunchDarkly rollout plan
├── TRACKING_PLAN.md          # Event tracking specifications
└── README.md                 # This file
```

### Key Directory Explanations

**routes/**: Contains file-based routes following Fresh conventions. Each `.tsx` file becomes a
route based on its path. The `_app.tsx` provides the application shell, while `_middleware.ts`
handles authentication.

**islands/**: Interactive components that hydrate on the client. Only these components receive
JavaScript. The island architecture keeps bundle sizes small while enabling interactivity where
needed.

**components/**: Server-rendered components with no client-side JavaScript. Used for static content
like navigation, footers, and non-interactive visualizations.

**utils/**: Shared utility modules organized by domain. The `launchdarkly/` subdirectory handles
feature flags and events. The `smarter/` subdirectory contains semantic layer logic. The `starter/`
subdirectory has pivot table utilities.

**static/**: Publicly accessible static files. The `smarter/` subdirectory contains semantic
metadata JSON files. The `starter_utils/` subdirectory has WebDataRocks assets and pivot metadata.

---

## LaunchDarkly Implementation

The LaunchDarkly implementation follows a four-stage rollout strategy with progressive feature
additions and comprehensive event tracking.

### Implementation Status

** Stage 1: Foundation & Core Tracking (COMPLETED)**

Established the experience routing infrastructure with the `analytics-experience` flag directing
users to Starter or Smarter experiences. Implemented the consolidated six-event tracking model
covering view, interaction, performance, session, operation, and error events. Added session
lifecycle tracking on the landing page and throughout the application.

Key deliverables:

- Experience routing with `analytics-experience` flag
- Consolidated event tracking system (6 event types)
- Session tracking on landing page
- Route change tracking throughout app

** Stage 2: Starter Experience Features (COMPLETED)**

Added feature gates and upgrade flows for the Starter experience. Implemented AI query access
control through `starter-ai-query-access` flag. Created reusable `UpgradeModal` component for
feature-based upsells. Added data limit enforcement with `starter-data-limit` flag triggering
upgrade prompts at the 1MB threshold.

Key deliverables:

- AI query gating with `starter-ai-query-access` flag
- `UpgradeModal` component for 5 feature types
- Data limit tracking with `starter-data-limit` flag
- Conversion funnel tracking for upgrade flows

** Stage 3: Smarter Experience Features (75% COMPLETE)**

Enhanced the Smarter experience with model tier selection, query validation strategies, and
persistence capabilities. The `smarter-model-tier` flag controls whether users receive the 3B or 7B
parameter WebLLM model. Query validation behavior is controlled by `query-validation-strategy` flag.
Query persistence through `smarter-query-persistence` enables IndexedDB storage. AI data analyst
assistant through `smarter-ai-analyst-access` provides automated insights.

Key deliverables:

-  Model tier gating with `smarter-model-tier` flag
-  Query validation strategy (needs UI work)
-  Query persistence with `smarter-query-persistence` flag
-  AI analyst assistant with `smarter-ai-analyst-access` flag

**⏳ Stage 4: Experiments & Analytics (PENDING)**

Will configure three experiments with LaunchDarkly metrics to measure impact on key business
outcomes. Metrics include query success rate, upgrade conversion rate, and feature engagement.
Experiments will test experience impact (Starter vs Smarter), model performance (3B vs 7B
with/without validation), and AI upsell effectiveness (gated vs open AI features).

Planned deliverables:

- 3 LaunchDarkly metrics configured
- 3 experiments running with proper allocation
- Custom analytics dashboard in LaunchDarkly
- SQL analysis queries for results

### Feature Flags Reference

**analytics-experience** (String, Multivariate)

- Controls primary experience routing
- Variations: `"starter"`, `"smarter"`
- Default: `"starter"`
- Targeting:
  - Email = specific addresses → `"smarter"`
  - Plan tier = "premium" → `"smarter"`
  - Queries executed > 50 → `"smarter"`
  - Gradual rollout: 0% → 100% over 4 weeks

**starter-ai-query-access** (Boolean)

- Gates AI-powered SQL generation in Starter
- Default: `false` (manual query builder only)
- When `true`: Natural language prompt interface enabled
- Use case: Test if AI increases engagement vs manual control

**starter-data-limit** (Number)

- Controls WebDataRocks data size limit (bytes)
- Variations:
  - `1048576` (1MB) - default
  - `5242880` (5MB)
  - `10485760` (10MB)
- Triggers upgrade prompt when exceeded
- Timeout: `(dataLimit / 1048576) * 10000` ms

**smarter-model-tier** (String)

- Controls which WebLLM model loads
- Variations:
  - `"3b"`: Qwen2.5-Coder-3B (faster load, lower accuracy)
  - `"7b"`: DeepSeek-R1-Distill-Qwen-7B (slower load, better reasoning)
- Default: `"7b"`
- Targeting: Users without WebGPU → `"3b"`

**query-validation-strategy** (String)

- Controls SQL validation behavior
- Variations:
  - `"strict"`: Always validate (3 retry attempts)
  - `"fast"`: Skip validation (fail immediately on error)
  - `"adaptive"`: Validate based on model (3B validates, 7B skips)
- Default: `"adaptive"`
- Metrics: Tracks attempts, validation usage, success rate

**smarter-query-persistence** (Boolean)

- Controls query storage location
- Default: `false` (sessionStorage only)
- When `true`: IndexedDB with full CRUD
- Upgrade prompt: After 3 queries in session mode

**smarter-ai-analyst-access** (Boolean)

- Gates AI insights and recommendations
- Default: `false` (basic KPIs only)
- When `true`: "Analyze Trends" button enabled
- Provides: Pattern detection, anomaly identification, actionable recommendations

### Event Tracking Schema

The consolidated six-event model captures all user and system interactions:

**view** - Anything displayed to users

```typescript
{
  type: "page" | "route" | "island" | "modal" | "section" | "results" | "preview" | "gate",
  context: string,        // e.g., "dashboard_routing", "landing"
  component: string,      // Component name
  plan: "starter" | "smarter" | null,
  route: string,          // Current path
  previousRoute: string,  // For funnel tracking
  referrer: string,       // Attribution
  contentType: string,    // Categorization
  itemCount: number,      // Result sets
  loadTimeMs: number      // Performance
}
```

**interaction** - User-initiated actions

```typescript
{
  action: "click" | "submit" | "navigate" | "switch" | "drag" | "hover" | "dismiss",
  target: string,         // Element clicked
  context: string,        // Where it happened
  component: string,      // Component name
  plan: string,
  route: string,
  destination: string,    // For navigation
  value: any,            // Form inputs
  previousValue: any,    // Comparisons
  timeInContext: number  // Engagement
}
```

**performance** - Metrics, warnings, errors

```typescript
{
  type: "load" | "error" | "warning" | "metric",
  context: string,
  component: string,
  plan: string,
  metric: string,        // Metric name
  value: number,         // Numeric value
  threshold: number,     // For alerts
  success: boolean,      // Outcome
  severity: "low" | "medium" | "high",
  errorType: string,     // Classification
  errorMessage: string   // Details
}
```

**session** - User session lifecycle

```typescript
{
  type: "start" | "end" | "idle",
  plan: string,
  sessionId: string,          // Correlation
  isReturning: boolean,       // User type
  daysSinceLastVisit: number,
  entryPage: string,          // Funnel start
  exitPage: string,           // Funnel end
  referrer: string,           // Attribution
  sessionDuration: number,    // Milliseconds
  queriesExecuted: number,    // Activity
  chartsGenerated: number,
  pagesViewed: number,
  interactionsCount: number,
  idleDuration: number,
  lastAction: string
}
```

**operation** - Server-side processes

```typescript
{
  type: "query" | "generation" | "materialization" | "computation",
  service: "duckdb" | "motherduck" | "semantic_layer" | "webllm",
  action: string,          // Description
  sql: string,            // Truncated to 500 chars
  executionTime: number,  // Milliseconds
  rowCount: number,       // Results
  cacheHit: boolean,      // Performance
  retryCount: number      // Reliability
}
```

**error** - Server failures

```typescript
{
  type: "timeout" | "exception" | "validation" | "connection",
  service: string,
  errorMessage: string,
  stackTrace: string,     // Truncated to 1000 chars
  requestId: string,      // Debugging
  userId: string,         // Impact assessment
  severity: "low" | "medium" | "high" | "critical",
  resolutionStatus: "open" | "investigating" | "resolved",
  timestamp: string
}
```

### LaunchDarkly Context Building

User context provides rich targeting capabilities:

```typescript
{
  kind: "multi",
  user: {
    key: string,              // Email or cookie ID
    name: string,             // Display name
    email: string,            // For targeting
    custom: {
      plan: "starter" | "smarter",
      betaGroup: boolean,     // Beta tester flag
      queriesExecuted: number,
      sqlExpertise: "novice" | "intermediate" | "advanced",
      daysSinceSignup: number,
      hasAIAccess: boolean,
      has7BModel: boolean,
      hasQueryPersistence: boolean,
      hasAIAnalyst: boolean
    }
  },
  device: {
    key: string,              // User agent hash
    kind: "device",
    platform: "web" | "mobile" | "desktop",
    browser: string,
    webGPUSupport: boolean    // For model targeting
  }
}
```

This enables sophisticated targeting:

- Show Smarter only to premium plan + 50 queries executed
- Enable 7B model only for browsers with WebGPU support
- Restrict persistence to beta testers initially
- Target AI analyst to high-engagement users (100+ queries)

---

## Key Features

### Semantic Layer Architecture

The semantic layer bridges the gap between business terminology and database schema, enabling
non-technical users to query data using familiar terms.

**Field Definitions** (Raw Database Columns):

Fields map directly to table columns with comprehensive metadata:

```json
{
  "plan_tier": {
    "md_data_type": "VARCHAR",
    "ingest_data_type": "string",
    "sanitize": false,
    "data_type_category": "categorical",
    "members": ["starter", "smarter", "premium", null],
    "group": "monetization",
    "parent": null
  },
  "session_revenue": {
    "md_data_type": "DOUBLE",
    "ingest_data_type": "number",
    "sanitize": false,
    "data_type_category": "measure",
    "members": null,
    "group": "monetization",
    "parent": null
  }
}
```

**Dimension Definitions** (User-Friendly Groupings):

Dimensions reference fields with optional transformations:

```json
{
  "plan_tier": {
    "alias_name": "Current Plan Tier",
    "transformation": "CASE WHEN plan_tier IS NULL THEN 'no plan' ELSE plan_tier END",
    "default_filters": [],
    "sort_order": "custom",
    "sort_values": ["no plan", "starter", "smarter", "premium"]
  },
  "session_date": {
    "alias_name": "Session Date",
    "transformation": null,
    "default_filters": ["DATE >= CURRENT_DATE - INTERVAL 30 DAYS"],
    "sort_order": "desc",
    "sort_values": null
  }
}
```

**Measure Definitions** (Business Metrics):

Measures specify aggregations with formatting:

```json
{
  "session_revenue": {
    "aggregations": [
      {
        "sum": {
          "alias": "Total Revenue",
          "format": "currency",
          "decimals": 2,
          "currency": "USD"
        }
      },
      {
        "avg": {
          "alias": "Average Deal Size",
          "format": "currency",
          "decimals": 2,
          "currency": "USD"
        }
      }
    ],
    "formula": {
      "revenue_per_session": {
        "sql": "SUM(session_revenue) / COUNT(DISTINCT session_id)",
        "alias": "Revenue Per Session",
        "format": "currency",
        "decimals": 2
      }
    }
  }
}
```

**Query Translation Example**:

User prompt: "show me average deal size by current plan tier for paid traffic"

Semantic translation:

1. "average deal size" → `AVG(session_revenue)` with currency formatting
2. "current plan tier" → `CASE WHEN plan_tier IS NULL THEN 'no plan' ELSE plan_tier END`
3. "paid traffic" → `WHERE traffic_source_type = 'paid'`

Generated SQL:

```sql
SELECT 
  CASE WHEN plan_tier IS NULL THEN 'no plan' ELSE plan_tier END as "Current Plan Tier",
  AVG(session_revenue) as "Average Deal Size"
FROM session_facts
WHERE traffic_source_type = 'paid'
GROUP BY 1
ORDER BY 2 DESC
```

The semantic layer handles all the complexity while the user works with familiar business terms.

### WebLLM Integration

WebLLM enables running large language models entirely in the browser, providing complete data
privacy and offline capability.

**Model Selection**:

The application supports two model tiers controlled by the `smarter-model-tier` flag:

**3B Model** (Qwen2.5-Coder-3B-Instruct-q4f16_1-MLC):

- Download size: ~1.8GB
- Memory footprint: ~2.5GB during inference
- Load time: 30-60 seconds after download
- Use case: Faster initialization, lower memory devices
- Accuracy: Good for simple queries, may struggle with complex joins

**7B Model** (DeepSeek-R1-Distill-Qwen-7B-q4f16_1-MLC):

- Download size: ~4.5GB
- Memory footprint: ~4.0GB during inference
- Load time: 60-90 seconds after download
- Use case: Better reasoning, complex query understanding
- Accuracy: Excellent for semantic understanding and edge cases

**Initialization Flow**:

```typescript
// SmartDashLoadingPage.tsx
const engine = await CreateMLCEngine(
  new Worker(new URL("./worker.ts", import.meta.url), { type: "module" }),
  modelId,
  {
    initProgressCallback: (progress) => {
      // Update progress bar: download → compile → warm-up
      console.log(`${progress.text}: ${progress.progress.toFixed(1)}%`);
    },
  },
);
```

Progress phases:

1. **Download** (0-60%): Model weights from CDN
2. **Compile** (60-90%): WebGPU shaders
3. **Warm-up** (90-100%): Sample generation for optimization

Model caches in IndexedDB at `mlc-chat-store` origin. Subsequent loads skip download and go straight
to compilation (~10 seconds).

**Query Generation**:

Prompt structure:

```typescript
const prompt = `You are an expert SQL analyst with access to the following table:

${generateSemanticMetadata(table)}

User question: "${userPrompt}"

Generate a query specification as JSON with this exact structure:
{
  "table": "${table}",
  "dimensions": ["dimension1", "dimension2"],
  "measures": ["measure1", "measure2"],
  "filters": ["condition1", "condition2"],
  "explanation": "Brief explanation"
}

CRITICAL: Return ONLY valid JSON. No markdown, no code blocks, no extra text.`;

const response = await engine.chat(prompt, {
  temperature: 0.0, // Deterministic output
  max_tokens: 300, // Concise responses
  top_p: 0.95,
});
```

**Validation and Retry**:

The semantic validator checks generated queries:

```typescript
const validationResult = validateAndAnalyzeQuery(querySpec, metadata);

if (!validationResult.isValid) {
  // Generate correction prompt
  const correction = generateCorrectionPrompt(
    validationResult.errors,
    metadata,
  );

  // Retry with feedback (max 3 attempts)
  const retryPrompt = `${originalPrompt}

Previous attempt had errors:
${correction}

Please generate a corrected query using only the available fields listed above.`;
}
```

Validation catches:

- Unknown dimension/measure names
- Incorrect aggregation syntax
- Invalid filter conditions
- Missing required fields

**Performance Optimization**:

- **Model Quantization**: 4-bit precision reduces memory by 75%
- **Prompt Caching**: Common metadata sections cached in context
- **Batching**: Multiple queries can batch through single model load
- **Worker Threading**: Inference runs in Web Worker to avoid blocking UI

### Chart Auto-Generation

The chart generation system analyzes query structure and data patterns to select optimal
visualizations automatically.

**Detection Logic**:

```typescript
// Query Response Analyzer
const analysis = {
  dimensions: categorizeDimensions(query.dimensions, metadata),
  measures: categorizeMeasures(query.measures, metadata),
  patterns: detectDataPatterns(data),
};

// Dimension categories
const dimensionTypes = {
  temporal: ["session_date", "first_event_date"],
  sequential: ["lifecycle_stage", "funnel_step"],
  categorical: ["plan_tier", "traffic_source"],
  binary: ["is_paying_customer", "has_activated"],
};

// Chart selection rules
if (hasSingleTemporal && hasMeasures) {
  return { type: "line", reasoning: "Time series data" };
}
if (hasSequential && hasSingleMeasure) {
  return { type: "funnel", reasoning: "Lifecycle progression" };
}
if (hasTwoCategorical && hasSingleMeasure) {
  return { type: "heatmap", reasoning: "Two-dimensional breakdown" };
}
if (hasSingleCategorical && hasMultipleMeasures) {
  return { type: "grouped-bar", reasoning: "Metric comparison by category" };
}
```

**Chart Configuration Generation**:

Each chart type receives optimized configuration:

**Line Charts** (Time Series):

```typescript
{
  type: "line",
  data: {
    labels: dates,
    datasets: measures.map(m => ({
      label: m.alias,
      data: values,
      borderColor: colorPalette[index],
      tension: 0.4,
      fill: false
    }))
  },
  options: {
    scales: {
      x: {
        type: "time",
        time: { unit: detectGranularity(dates) }
      },
      y: {
        beginAtZero: true,
        ticks: { callback: formatCurrency }
      }
    },
    plugins: {
      tooltip: {
        callbacks: {
          label: formatTooltip
        }
      }
    }
  }
}
```

**Funnel Charts** (Lifecycle):

```typescript
{
  type: "funnel",
  data: {
    labels: stages,
    datasets: [{
      data: counts,
      backgroundColor: gradientColors
    }]
  },
  options: {
    sort: "descending",
    percentages: true,
    gap: 2
  }
}
```

**Heatmap Charts** (Two Dimensions):

```typescript
{
  type: "heatmap",
  data: createMatrix(rows, cols, values),
  options: {
    colorScale: {
      type: "diverging",
      colors: ["#d73027", "#f7f7f7", "#1a9850"],
      domain: [min, median, max]
    },
    annotations: {
      display: values.length < 100  // Show values if not too dense
    }
  }
}
```

**Smart Formatting**:

The system applies appropriate formatting based on measure metadata:

- **Currency**: `$1,234.56` with locale-aware formatting
- **Percentage**: `12.5%` with configurable decimal places
- **Numbers**: `1,234` with thousand separators
- **Dates**: `Jan 15, 2024` with relative formatting (`2 days ago`)

### Query Persistence System

The persistence system provides seamless query history management with progressive enhancement based
on feature flags.

**Storage Modes**:

**Session Storage** (Default):

```typescript
// Only persists for current browser session
const queries = JSON.parse(sessionStorage.getItem("session_queries") || "[]");
queries.push(newQuery);
sessionStorage.setItem("session_queries", JSON.stringify(queries));

// Limit to 10 queries with LRU eviction
if (queries.length > 10) {
  queries.shift(); // Remove oldest
}
```

**IndexedDB Storage** (Flag Enabled):

```typescript
// Persistent across sessions with full CRUD
const db = await openDB("dasgata_queries", 1, {
  upgrade(db) {
    const store = db.createObjectStore("queries", {
      keyPath: "id",
      autoIncrement: true,
    });
    store.createIndex("timestamp", "timestamp");
    store.createIndex("table", "table");
    store.createIndex("tags", "tags", { multiEntry: true });
  },
});

// Save with metadata
await db.add("queries", {
  prompt: userPrompt,
  sql: generatedSQL,
  querySpec: { dimensions, measures, filters },
  results: data,
  chartConfig: config,
  timestamp: Date.now(),
  table: tableName,
  tags: userTags,
  notes: userNotes,
  executionMetadata: {
    success: true,
    duration: executionTime,
    rowCount: data.length,
  },
});
```

**SavedQueries Component**:

Provides UI for query management:

```tsx
<SavedQueries>
  {/* List view with search and filters */}
  <QueryList
    queries={savedQueries}
    onSelect={handleSelect}
    onDelete={handleDelete}
    onReplay={handleReplay}
    sortBy="timestamp"
    filterBy={selectedTags}
  />

  {/* Preview panel */}
  {selectedQuery && (
    <QueryPreview
      query={selectedQuery}
      showResults={true}
      showChart={true}
    />
  )}

  {/* Upgrade prompt for non-persistent users */}
  {!canPersist && queries.length >= 3 && (
    <UpgradeModal
      feature="query_persistence"
      trigger="query_limit"
    />
  )}
</SavedQueries>;
```

**Query Replay**:

Saved queries can be replayed with updated data:

```typescript
async function replayQuery(savedQuery) {
  // Use original query spec, not SQL
  const freshData = await semanticTable.query(savedQuery.querySpec);

  // Regenerate chart with new data
  const chartConfig = generateChartFromAnalysis(
    savedQuery.querySpec,
    freshData,
    metadata,
  );

  return { data: freshData, chart: chartConfig };
}
```

This ensures queries adapt to schema changes in the semantic layer.

**Export/Import**:

Users can share queries as JSON:

```json
{
  "version": "1.0",
  "prompt": "Show me revenue by plan tier",
  "querySpec": {
    "table": "sessions",
    "dimensions": ["plan_tier"],
    "measures": ["session_revenue"],
    "filters": ["session_date >= CURRENT_DATE - 30"]
  },
  "chartConfig": {/* Chart.js config */},
  "metadata": {
    "created": "2024-11-23T10:30:00Z",
    "author": "user@example.com"
  }
}
```

### AI Data Analyst Assistant

The AI analyst transforms raw metrics into actionable insights using WebLLM to analyze patterns and
recommend actions.

**Insight Generation**:

The assistant analyzes KPI data to identify trends:

```typescript
async function generateInsights(kpis, webllmEngine) {
  const prompt = `Analyze these marketing metrics and provide insights:

KPIs:
- Activation Rate: ${kpis.activationRate.current}% (previous: ${kpis.activationRate.previous}%)
- Average Deal Size: $${kpis.avgDealSize.current} (previous: $${kpis.avgDealSize.previous})
- Active Customers: ${kpis.activeCustomers.current} (previous: ${kpis.activeCustomers.previous})

Provide:
1. Key trends (what's changing and why it matters)
2. Patterns (unusual movements or correlations)
3. Recommendations (2-3 specific actions)

Format as JSON:
{
  "insights": ["trend 1", "trend 2"],
  "patterns": ["pattern 1", "pattern 2"],
  "recommendations": ["action 1", "action 2", "action 3"]
}`;

  const response = await webllmEngine.chat(prompt);
  return JSON.parse(response);
}
```

**Pattern Detection**:

The system identifies statistically significant changes:

- **Threshold Alerts**: >10% change in key metrics
- **Cohort Analysis**: Retention curve shifts
- **Channel Performance**: Traffic source changes
- **Conversion Drops**: Funnel stage decreases

**UI Integration**:

```tsx
<LandingOverview>
  {/* Standard KPI cards */}
  <KPIGrid>
    <KPICard metric="activationRate" />
    <KPICard metric="avgDealSize" />
    <KPICard metric="activeCustomers" />
  </KPIGrid>

  {/* AI Analyst Section */}
  {hasAIAnalyst
    ? (
      <AIInsights
        insights={analysisResult}
        onAnalyze={handleAnalyzeClick}
        isLoading={isAnalyzing}
      />
    )
    : (
      <div className="ai-analyst-gate">
        <h3> AI Data Analyst</h3>
        <p>Get automated insights and recommendations</p>
        <button onClick={() => setShowUpgrade(true)}>
          Unlock AI Analyst
        </button>
      </div>
    )}
</LandingOverview>;
```

**Access Control**:

The `smarter-ai-analyst-access` flag controls visibility:

```typescript
const hasAIAnalyst = ldClient.variation("smarter-ai-analyst-access", false);

// Event tracking
if (!hasAIAnalyst) {
  trackView("gate", "feature_gate", "LandingOverview", {
    feature: "ai_analyst_assistant",
    gateType: "premium_feature",
  });
}

// Usage tracking
if (hasAIAnalyst && userClickedAnalyze) {
  trackInteraction("click", "ai_analyst_button", "analysis", "LandingOverview", {
    sessionCount: sessionData.length,
    analysisType: "kpi_overview",
  });
}
```

**Example Output**:

```json
{
  "insights": [
    "Activation rate increased 15% from 42% to 57%, indicating improved onboarding flow effectiveness",
    "Average deal size decreased 8% from $127 to $117, suggesting shift toward lower-tier plans",
    "Active customer count remained stable at ~2,100, showing strong retention despite plan shifts"
  ],
  "patterns": [
    "Inverse correlation between activation rate and deal size suggests new activation strategy attracts more price-sensitive users",
    "Stable active customer count despite lower deal sizes indicates volume growth offsetting value decline"
  ],
  "recommendations": [
    "Investigate recent onboarding changes to understand activation improvements and document best practices",
    "Analyze new customer cohorts to determine if lower deal sizes are temporary (trial periods) or permanent",
    "Consider introducing mid-tier pricing between current plans to capture value from activated price-sensitive users"
  ]
}
```

---

## Development Workflow

### Local Development

Start the development server with watch mode:

```bash
deno task start
```

The server automatically reloads on file changes. Fresh's island architecture means only modified
islands re-hydrate, providing sub-second refresh times for most changes.

**Development URLs**:

- Landing page: `http://localhost:8000`
- Starter dashboard: `http://localhost:8000/app/dashboard/starter`
- Smarter dashboard: `http://localhost:8000/app/dashboard/smarter`

**Hot Reload Behavior**:

- Route changes: Full page reload
- Island changes: Component re-hydration only
- Component changes: No reload (server-rendered)
- Util changes: Full page reload

### Testing Feature Flags Locally

Override flags for testing without changing LaunchDarkly dashboard:

```typescript
// Create test context
const testContext = {
  kind: "multi",
  user: {
    key: "test-user-123",
    email: "test@example.com",
    custom: {
      plan: "smarter",
      queriesExecuted: 150,
      hasAIAnalyst: true,
    },
  },
  device: {
    key: "test-device",
    kind: "device",
    webGPUSupport: true,
  },
};

// Use in flag evaluation
const experience = client.variation("analytics-experience", "starter", testContext);
```

**Testing Different User States**:

```typescript
// New user
const newUser = { ...baseContext, custom: { queriesExecuted: 0 } };

// Power user
const powerUser = { ...baseContext, custom: { queriesExecuted: 500 } };

// Non-WebGPU browser
const oldBrowser = { ...baseContext, device: { webGPUSupport: false } };
```

### Debugging Semantic Queries

Enable verbose logging in semantic-config.ts:

```typescript
const DEBUG_MODE = true;

// Logs will show:
//  User prompt
//  Semantic metadata sent to LLM
//  Raw model response
//  Parsed query specification
//  Validation results
//  Correction prompts (if validation fails)
//  Generated SQL
//  Execution time
//  Result row count
//  Chart configuration
```

**Common Issues and Solutions**:

**Issue**: Model returns unknown field names

```
Error: Unknown dimension "plan_type" (did you mean "plan_tier"?)
```

Solution: Check that semantic metadata includes all field aliases. Regenerate metadata from database
if stale.

**Issue**: Aggregation syntax errors

```
Error: Invalid SQL - COUNT(DISTINCT cookie_id) as user_count
```

Solution: Ensure measures use correct aggregation wrappers. Check formula syntax in metadata.

**Issue**: Filter formatting errors

```
Error: Date filter "last 30 days" not recognized
```

Solution: Semantic validator requires explicit date format. Use ISO strings or CURRENT_DATE syntax.

### WebLLM Development Tips

**Force Fresh Model Download**:

Clear IndexedDB to re-download model:

1. Open DevTools → Application → Storage
2. Expand IndexedDB → `mlc-chat-store`
3. Right-click → Delete database
4. Reload page to trigger fresh download

**Adjust Generation Parameters**:

```typescript
const response = await engine.chat(prompt, {
  temperature: 0.0, // 0.0 = deterministic, 1.0 = creative
  max_tokens: 300, // Increase for longer explanations
  top_p: 0.95, // Nucleus sampling threshold
  frequency_penalty: 0.0,
  presence_penalty: 0.0,
});
```

**Prompt Engineering Best Practices**:

1. **Be Specific**: Mention exact table and field names
2. **Show Format**: Include example JSON structure
3. **Constrain Output**: Explicitly say "JSON only, no markdown"
4. **Provide Context**: Include relevant dimension/measure lists
5. **Handle Errors**: Give feedback on previous failures

**Performance Profiling**:

Use Chrome DevTools Performance panel:

1. Start recording
2. Trigger WebLLM generation
3. Stop recording
4. Analyze flame graph for:
   - WASM execution time
   - GPU shader compilation
   - JavaScript bridge overhead
   - Memory allocations

Typical timings:

- Model initialization: 60-90 seconds (first time), 10 seconds (cached)
- Single query generation: 1-3 seconds
- Memory peak: 4-5 GB during inference

### Testing Data Limits

**Trigger Upgrade Flow**:

Create query that exceeds limit:

```typescript
// In BaseDashboardWizard
const largeQuery = `
  SELECT * FROM sessions_pivot_src 
  WHERE session_date >= CURRENT_DATE - INTERVAL 180 DAYS
  LIMIT 10000
`;

// Will exceed 1MB limit and trigger upgrade modal
```

**Test Different Limits**:

Adjust LaunchDarkly flag `starter-data-limit`:

- 1MB (1048576): ~5,000 rows typical result
- 5MB (5242880): ~25,000 rows typical result
- 10MB (10485760): ~50,000 rows typical result

**Verify Upgrade Tracking**:

Check LaunchDarkly events for:

```typescript
trackView("gate", "feature_gate", "WebDataRocksPivot", {
  feature: "data_limit",
  gateType: "plan_upgrade",
  currentLimit: 1048576,
  dataSize: actualSize,
});

trackInteraction("click", "upgrade_button", "upsell", "UpgradeModal", {
  feature: "data_limit",
  trigger: "data_limit",
});
```

### Performance Profiling

**Identify Bottlenecks**:

1. **Model Loading**: First time ~3-5 minutes, cached ~10 seconds
2. **Query Execution**: <1 second for typical result sets
3. **Chart Rendering**: <500ms for <10K data points
4. **Memory Usage**: 4-5GB during WebLLM inference

**Optimize Strategies**:

- **Data Sampling**: For large result sets, downsample before charting
- **Lazy Loading**: Defer non-visible islands
- **Caching**: Use IndexedDB for query results
- **Web Workers**: Keep inference off main thread

---

### Coding Standards

**TypeScript Style**:

- Use explicit types for function parameters and returns
- Avoid `any` except when interfacing with untyped libraries
- Prefer `interface` for object shapes, `type` for unions
- Use `readonly` for immutable data structures

```typescript
// Good
interface QuerySpec {
  readonly table: string;
  readonly dimensions: readonly string[];
  readonly measures: readonly string[];
}

function executeQuery(spec: QuerySpec): Promise<QueryResult> {
  // ...
}

// Avoid
function executeQuery(spec: any): any {
  // ...
}
```

**Component Organization**:

- Islands (client-side): `islands/` directory, must be default exports
- Components (server-side): `components/` directory
- API routes: `routes/api/` with exported `handler` function
- Utilities: `utils/` organized by domain

**Naming Conventions**:

- Components: `PascalCase` (e.g., `SessionDashboard.tsx`)
- Variables/functions: `camelCase` (e.g., `executeQuery`)
- Constants: `SCREAMING_SNAKE_CASE` (e.g., `MAX_RETRIES`)
- Files: `kebab-case` for non-components (e.g., `semantic-config.ts`)

**JSDoc for Public APIs**:

```typescript
/**
 * Validates a semantic query against table metadata.
 *
 * @param querySpec - The query specification from WebLLM
 * @param metadata - Table metadata with dimension and measure definitions
 * @returns Validation result with errors and corrections
 *
 * @example
 * const result = validateQuery(
 *   { dimensions: ["plan_tier"], measures: ["revenue"] },
 *   sessionsMetadata
 * );
 */
export function validateQuery(
  querySpec: QuerySpec,
  metadata: SemanticMetadata,
): ValidationResult {
  // ...
}
```

**Event Tracking**:

Use the consolidated 6-event model consistently:

```typescript
// Component mount
trackView("island", "dashboard_overview", "LandingOverview", {
  loadTimeMs: performance.now(),
});

// User interaction
trackInteraction("click", "analyze_button", "insights", "LandingOverview", {
  kpiCount: kpis.length,
});

// Performance metric
trackPerformance("metric", "webllm_generation", "WebLLMHandler", {
  metric: "generation_time",
  value: duration,
  success: true,
  modelTier: "7b",
});
```

### Common Contribution Areas

**Dashboard Queries**:

Add new pre-built analysis patterns to `semantic-dashboard-queries.ts`:

```typescript
{
  id: "cohort_retention_by_source",
  table: "users",
  dimensions: ["first_touch_source", "cohort_month"],
  measures: ["user_count", "active_users_30d"],
  filters: ["cohort_month >= CURRENT_DATE - INTERVAL 12 MONTHS"],
  chartType: "heatmap",
  title: "Cohort Retention by Acquisition Source",
  description: "Track how different traffic sources perform over time"
}
```

**Semantic Layer Enhancements**:

Add dimensions or measures to metadata files:

```json
{
  "session_duration_minutes": {
    "md_data_type": "DOUBLE",
    "ingest_data_type": "number",
    "data_type_category": "measure",
    "formula": {
      "sql": "session_duration_seconds / 60.0",
      "alias": "Session Duration (Minutes)",
      "format": "number",
      "decimals": 1
    }
  }
}
```

**Visualization Improvements**:

Implement new chart types in `chart-generator.ts`:

```typescript
if (hasTwoMeasures && noIntersectingDimensions) {
  return {
    type: "scatter",
    reasoning: "Correlation analysis between two metrics",
    xKey: measures[0],
    yKey: measures[1],
  };
}
```

**Performance Optimizations**:

- Add query result caching in semantic-objects.ts
- Implement data downsampling for large charts
- Optimize WebLLM prompt lengths
- Add progressive loading for dashboard KPIs

**Accessibility Improvements**:

- Add ARIA labels to interactive elements
- Ensure keyboard navigation works everywhere
- Test with screen readers
- Add focus indicators to all interactive elements
- Implement skip links for dashboard sections

### Documentation Contributions

Documentation improvements are highly valued:

**README Updates**:

- Add new features to feature list
- Update installation steps if dependencies change
- Add troubleshooting entries for common issues
- Improve examples with real-world use cases

**Code Documentation**:

- Add JSDoc to exported functions
- Explain complex algorithms with inline comments
- Document non-obvious implementation decisions
- Add usage examples to utility functions

**Wiki Content**:

- Create tutorials for common workflows
- Document best practices for semantic layer design
- Write guides for LaunchDarkly experimentation
- Share performance optimization techniques

---

## Troubleshooting

### WebLLM Fails to Load

**Symptoms**: Model initialization hangs, errors in console, infinite loading screen

**Diagnostics**:

1. Check WebGPU support: Visit `https://webgpureport.org/`
2. Verify browser version: Chrome 113+, Edge 113+, Safari 17+
3. Check available RAM: 8GB+ recommended, 4GB minimum
4. Inspect console for specific errors

**Common Issues**:

**CORS Errors**:

```
Access to fetch at 'https://...' from origin 'http://localhost:8000' has been blocked
```

Solution: Ensure service worker (sw.js) is properly registered. Check that static files are served
correctly.

**Out of Memory**:

```
WebGPU: Out of memory creating buffer
```

Solution: Close other tabs, use smaller model (3B instead of 7B), increase browser memory limit in
chrome://flags.

**WebGPU Not Available**:

```
navigator.gpu is undefined
```

Solution: Enable "Unsafe WebGPU" flag in chrome://flags (development only). Update to latest browser
version. Check GPU drivers are up to date.

**IndexedDB Quota Exceeded**:

```
QuotaExceededError: The quota has been exceeded
```

Solution: Clear browser storage. Request persistent storage with `navigator.storage.persist()`. Use
smaller model.

### Semantic Query Validation Fails

**Symptoms**: Repeated "Unknown field" errors, queries fail validation even when they look correct

**Diagnostics**:

Enable debug logging:

```typescript
// semantic-config.ts
const DEBUG = true;
```

Check validation logs for specific errors:

```
 [VALIDATION ERROR] Unknown dimension "plan_type"
 Did you mean: "plan_tier" (alias: "Current Plan Tier")
```

**Common Issues**:

**Case Sensitivity**:

```
Error: Unknown dimension "Plan_Tier"
```

Solution: Field names are case-sensitive. Use exact key from metadata: `plan_tier` not `Plan_Tier`.

**Alias vs Source Name**:

```
Error: Unknown dimension "Current Plan Tier"
```

Solution: Model must use source field keys, not alias names. Check metadata fields section for
correct key.

**Stale Metadata**:

```
Error: Unknown dimension "new_field_added_yesterday"
```

Solution: Regenerate metadata from database:

```sql
SUMMARIZE amplitude.sessions_pivot_src;
SUMMARIZE amplitude.users_fct;
```

Update JSON files with new schema.

**Aggregation Syntax**:

```
Error: Invalid aggregation "COUNT(DISTINCT cookie_id)"
```

Solution: Use measure formulas, not direct SQL in query spec. Reference measure by alias:
`"unique_visitors"`.

### MotherDuck Connection Issues

**Symptoms**: "Table not found" errors, connection timeouts, materialization failures

**Diagnostics**:

1. Check token validity: Log into motherduck.com
2. Verify database exists: `my_db.amplitude`
3. Test table access: `SELECT COUNT(*) FROM my_db.amplitude.sessions_pivot_src`
4. Check network: Corporate firewall may block motherduck.com

**Common Issues**:

**Invalid Token**:

```
Error: Authentication failed
```

Solution: Regenerate token in MotherDuck dashboard. Update .env file. Restart dev server.

**Table Not Found**:

```
Error: Table "sessions_pivot_src" does not exist
```

Solution: Check full table path: `my_db.amplitude.sessions_pivot_src`. Verify database name in
MotherDuck. Ensure materialization completed successfully.

**Timeout During Materialization**:

```
Error: Query timeout after 30000ms
```

Solution: Increase timeout in SmartDashLoadingPage:

```typescript
const timeout = 60000; // 60 seconds instead of 30
```

**Streaming Errors**:

```
Error: dataReader.read() failed
```

Solution: Use simple CREATE TABLE AS SELECT instead of streaming. Update SmartDashLoadingPage to use
non-streaming approach.

### Chart Rendering Issues

**Symptoms**: Blank charts, distorted visuals, incorrect axis labels, crashes

**Diagnostics**:

1. Check browser console for Chart.js errors
2. Inspect data passed to chart component
3. Verify Chart.js configuration structure
4. Test with minimal dataset first

**Common Issues**:

**BigInt Serialization**:

```
TypeError: Do not know how to serialize a BigInt
```

Solution: Convert BigInt to Number before charting:

```typescript
const sanitized = data.map((row) => ({
  ...row,
  count: Number(row.count), // Convert BigInt
}));
```

**Invalid Date Format**:

```
Error: Invalid time value
```

Solution: Convert DuckDB date objects:

```typescript
if (val && typeof val === "object" && "days" in val) {
  return new Date(val.days * 24 * 60 * 60 * 1000);
}
```

**Empty Dimension Labels**:

```
X-axis shows: 0, 1, 2, 3 instead of "starter", "smarter", "premium"
```

Solution: Ensure dimensions array is populated from GROUP BY:

```typescript
// In semantic-query-validator.ts
const groupByColumns = extractGroupByColumns(sql);
const dimensions = matchColumnsToDimensions(groupByColumns, metadata);
```

**Performance Issues with Large Datasets**:

```
Chart freezes browser with 50K data points
```

Solution: Implement client-side aggregation:

```typescript
if (data.length > 10000) {
  // Bin data or sample
  data = downsampleData(data, 1000);
}
```

### LaunchDarkly Events Not Appearing

**Symptoms**: Events don't show in LaunchDarkly dashboard, experiments have no data

**Diagnostics**:

1. Check SDK initialization: `getLDClient()` returns non-null
2. Verify client ID in .env: Matches LaunchDarkly dashboard
3. Check network tab: Requests to `events.launchdarkly.com`
4. Test with debugger: LaunchDarkly dashboard → Debugger tab

**Common Issues**:

**SDK Not Initialized**:

```
Error: Cannot call track() before client is initialized
```

Solution: Wait for initialization:

```typescript
await ldClient.waitForInitialization();
```

**Wrong Environment**:

```
Events appear in test environment, not production
```

Solution: Check client ID corresponds to correct environment in LaunchDarkly.

**Event Schema Mismatch**:

```
Events tracked but don't appear in metrics
```

Solution: Verify event key matches metric configuration exactly. Check that custom properties match
metric filters.

**CORS Blocking**:

```
Failed to fetch events.launchdarkly.com (blocked by CORS)
```

Solution: Check browser extensions (ad blockers). Verify CSP headers allow LaunchDarkly domains.
Test in incognito mode.

**Buffering Delay**:

```
Events eventually appear but with 5-10 minute delay
```

Solution: This is normal. LaunchDarkly aggregates events before displaying. Use Debugger for
real-time validation.

### TypeScript Compilation Errors

**Symptoms**: Module not found, type errors, cannot find name

**Diagnostics**:

Run type checker:

```bash
deno check **/*.ts **/*.tsx
```

Check import paths and module specifiers.

**Common Issues**:

**Missing File Extension**:

```
error: Module not found "file:///utils/semantic-config"
```

Solution: Include .ts extension in imports:

```typescript
// Wrong
import { getSemanticConfig } from "../utils/semantic-config";

// Correct
import { getSemanticConfig } from "../utils/semantic-config.ts";
```

**npm Package Import**:

```
error: Could not find npm package "@motherduck/wasm-client"
```

Solution: Add to deno.json imports:

```json
{
  "imports": {
    "@motherduck/wasm-client": "npm:@motherduck/wasm-client@^0.7.0"
  }
}
```

**Type Definition Missing**:

```
error: Property 'variation' does not exist on type 'unknown'
```

Solution: Install type definitions or use type assertion:

```typescript
const client = getLDClient() as LDClient;
```

**Circular Dependency**:

```
error: Circular dependency detected
```

Solution: Extract shared types to separate file. Reorganize imports to break cycle.

---

## Additional Resources

### Project Documentation

- **IMPLEMENTATION_PLAN.md**: Detailed 4-stage LaunchDarkly rollout plan with flag configs and
  success metrics
- **TRACKING_PLAN.md**: Complete event taxonomy with 70+ event types and schemas

### External Documentation

- **Deno Fresh**: https://fresh.deno.dev - Framework concepts, routing, islands
- **DuckDB**: https://duckdb.org/docs - SQL syntax, functions, performance tuning
- **WebLLM**: https://mlc.ai/web-llm - Model selection, initialization, generation APIs
- **LaunchDarkly**: https://docs.launchdarkly.com - Feature flags, targeting, experiments
- **Chart.js**: https://www.chartjs.org - Configuration, plugins, customization
- **MotherDuck**: https://motherduck.com/docs - Cloud warehouse, authentication, streaming

### Community

- **GitHub Issues**: Bug reports and feature requests
- **Discord Server**: Real-time chat with maintainers and contributors
- **Blog**: Development updates and technical deep dives
- **Changelog**: Release notes and upgrade guides

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

Built with love using:

- **Deno Fresh** - Modern web framework with island architecture
- **DuckDB** - Blazing-fast in-browser analytics
- **MotherDuck** - Cloud data warehouse built on DuckDB
- **WebLLM** - Privacy-preserving browser-based AI inference
- **Chart.js** - Beautiful interactive visualizations
- **LaunchDarkly** - Sophisticated feature management and experimentation

Inspired by modern data stack principles, semantic layer architectures, and the democratization of
data analytics.
