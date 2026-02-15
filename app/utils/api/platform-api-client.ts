// Platform API Client — interface to the FastAPI backend via Deno proxy

// --- TypeScript Interfaces (mirror FastAPI Pydantic models) ---

export interface ModelSummary {
  name: string;
  label: string;
  description: string;
  dimension_count: number;
  measure_count: number;
  has_joins: boolean;
}

export interface DimensionDef {
  name: string;
  type: string;
}

export interface MeasureDef {
  name: string;
  type: string;
  agg: string;
  label?: string;
}

export interface CalculatedMeasureDef {
  name: string;
  label: string;
  sql: string;
  format?: string;
}

export interface JoinDef {
  to: string;
  type: string;
  on: Record<string, string>;
}

export interface ModelDetail {
  name: string;
  label: string;
  description: string;
  dimensions: DimensionDef[];
  measures: MeasureDef[];
  calculated_measures: CalculatedMeasureDef[];
  joins: JoinDef[];
}

export interface QueryFilter {
  field: string;
  op: string;
  value: string | number | (string | number)[] | null;
}

export interface OrderByClause {
  field: string;
  dir: "asc" | "desc";
}

export interface SemanticQueryRequest {
  model: string;
  dimensions: string[];
  measures: string[];
  calculated_measures: string[];
  filters: QueryFilter[];
  joins: string[];
  order_by: OrderByClause[];
  limit: number | null;
}

export interface ColumnInfo {
  name: string;
  type: string;
}

export interface SemanticQueryResponse {
  sql: string;
  data: Record<string, unknown>[];
  columns: ColumnInfo[];
  row_count: number;
}

export interface ObservabilitySummary {
  tenant_slug: string;
  models_count: number;
  last_run_at: string | null;
  pass_count: number;
  fail_count: number;
  error_count: number;
  skip_count: number;
  avg_execution_time: number;
}

export interface RunResult {
  model_name: string;
  status: string;
  rows_affected: number | null;
  execution_time_seconds: number;
  run_started_at: string;
}

export interface TestResult {
  test_name: string;
  status: string;
  message: string | null;
  execution_time_seconds: number;
  run_started_at: string;
}

export interface IdentityResolutionStats {
  tenant_slug: string;
  total_users: number;
  resolved_customers: number;
  anonymous_users: number;
  resolution_rate: number;
  total_events: number;
  total_sessions: number;
}

export interface AskRequest {
  question: string;
  max_records?: number;
  semantic_context?: string;
}

export interface AskResponse {
  answer: string;
  records: Record<string, unknown>[];
  sql: string;
  chart_spec: Record<string, unknown> | null;
  model_used: string;
  provider: string;
  execution_time_ms: number;
  tool_calls: string[];
  error: string | null;
}

export interface BackendLLMStatus {
  available: boolean;
  provider: string;
  model: string;
}

export interface BSLConfig {
  tenant: {
    slug: string;
    business_name: string;
    source_platforms: Record<string, string[]>;
  };
  models: ModelDetail[];
}


export interface ReadinessStatus {
  is_ready: boolean;
  last_load_id: string | null;
  status: 'starting' | 'ingesting' | 'modeling' | 'cataloging' | 'ready' | 'error';
  message?: string;
  last_dbt_status?: string;
}

// --- API Client ---

export class PlatformAPIClient {
  private baseUrl: string;

  constructor(baseUrl = "") {
    this.baseUrl = baseUrl;
  }

  private async request<T>(path: string, options?: RequestInit): Promise<T> {
    const url = `${this.baseUrl}/api/platform/${path}`;
    const response = await fetch(url, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...options?.headers,
      },
    });

    if (!response.ok) {
      const errorBody = await response.text();
      let detail = errorBody;
      try {
        const parsed = JSON.parse(errorBody);
        detail = parsed.detail || parsed.error || errorBody;
      } catch {
        // use raw text
      }
      console.error(`Platform API error [${response.status}]: ${detail}`);
      throw new Error(`Platform API error: ${detail}`);
    }

    return response.json();
  }

  async checkReadiness(tenantSlug: string): Promise<ReadinessStatus> {
    return this.request<ReadinessStatus>(`readiness/${tenantSlug}`);
  }

  async getModels(tenantSlug: string): Promise<ModelSummary[]> {
    return this.request<ModelSummary[]>(`semantic-layer/${tenantSlug}/models`);
  }

  async getModelDetail(tenantSlug: string, modelName: string): Promise<ModelDetail> {
    return this.request<ModelDetail>(`semantic-layer/${tenantSlug}/models/${modelName}`);
  }

  async getBSLConfig(tenantSlug: string): Promise<BSLConfig> {
    return this.request<BSLConfig>(`semantic-layer/${tenantSlug}/config`);
  }

  async executeQuery(tenantSlug: string, query: SemanticQueryRequest): Promise<SemanticQueryResponse> {
    return this.request<SemanticQueryResponse>(`semantic-layer/${tenantSlug}/query`, {
      method: "POST",
      body: JSON.stringify(query),
    });
  }

  async getObservabilitySummary(tenantSlug: string): Promise<ObservabilitySummary> {
    return this.request<ObservabilitySummary>(`observability/${tenantSlug}/summary`);
  }

  async getRunResults(tenantSlug: string, limit?: number): Promise<RunResult[]> {
    const query = limit ? `?limit=${limit}` : "";
    return this.request<RunResult[]>(`observability/${tenantSlug}/runs${query}`);
  }

  async getTestResults(tenantSlug: string, limit?: number): Promise<TestResult[]> {
    const query = limit ? `?limit=${limit}` : "";
    return this.request<TestResult[]>(`observability/${tenantSlug}/tests${query}`);
  }

  async getIdentityResolution(tenantSlug: string): Promise<IdentityResolutionStats> {
    return this.request<IdentityResolutionStats>(`observability/${tenantSlug}/identity-resolution`);
  }

  async askQuestion(tenantSlug: string, request: AskRequest): Promise<AskResponse> {
    return this.request<AskResponse>(`semantic-layer/${tenantSlug}/ask`, {
      method: "POST",
      body: JSON.stringify(request),
    });
  }

  async checkBackendLLM(): Promise<BackendLLMStatus> {
    try {
      const result = await this.request<{ provider: string; model: string; is_available: boolean; error: string }>(
        "health/llm"
      );
      return {
        available: result.is_available,
        provider: result.provider,
        model: result.model,
      };
    } catch {
      return { available: false, provider: "none", model: "" };
    }
  }
}


// --- Factory ---

export function createPlatformAPIClient(): PlatformAPIClient {
  return new PlatformAPIClient("");
}
