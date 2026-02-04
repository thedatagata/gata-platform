// utils/smarter/autovisualization_dashboard/webllm-handler.ts
import { createQueryAnalyzer } from "../dashboard_utils/query-response-analyzer.ts";
import { CreateMLCEngine } from "@mlc-ai/web-llm";
import { generateSQLPrompt } from "../dashboard_utils/semantic-config.ts";
import { createQueryValidator } from "../dashboard_utils/semantic-query-validator.ts";
import type { SemanticReportObj } from "../dashboard_utils/semantic-objects.ts";
import type { PinnedItem } from "../dashboard_utils/query-persistence.ts";

export interface QueryResponse {
  table: string;
  dimensions: Array<{ alias: string; sourceField: string }>;
  measures: Array<{ alias: string; sourceField: string }>;
  filters?: string[];
  explanation: string;
  sql: string;
}

export interface QueryGenerationMetrics {
  attemptCount: number;
  validationUsed: boolean;
  initialValid: boolean;
  finalValid: boolean;
  totalTimeMs: number;
  errorTypes?: string[];
  executionError?: string;
}

export class WebLLMSemanticHandler {
  private engine: any;
  private modelId: string;
  private useValidation: boolean;
  private maxTokens: number;
  private temperature: number;

  private static MODEL_TIERS = {
    "3b": "Qwen2.5-Coder-3B-Instruct-q4f16_1-MLC",
    "7b": "Qwen2.5-Coder-7B-Instruct-q4f16_1-MLC"
  };

  constructor(
    private semanticTables: Record<string, SemanticReportObj>,
    tier: "3b" | "7b" = "3b",
    private ldClient?: { variation: (key: string, defaultValue: unknown) => unknown; track: (event: string, meta: unknown) => void }
  ) {
    let selectedTier: "3b" | "7b" = tier;
    
    if (ldClient) {
      const flagTier = ldClient.variation("smarter-model-tier", tier);
      if (flagTier === "3b" || flagTier === "7b") {
        selectedTier = flagTier;
      }
    }

    this.modelId = WebLLMSemanticHandler.MODEL_TIERS[selectedTier];
    this.maxTokens = 1000;
    this.temperature = 0.0;
    this.useValidation = selectedTier === "3b";
  }

  public registerTable(tableName: string, reportObj: SemanticReportObj) {
    this.semanticTables[tableName] = reportObj;
  }

  async initialize(onProgress?: (progress: { progress: number; text: string }) => void) {
    const startTime = performance.now();
    this.engine = await CreateMLCEngine(this.modelId, {
      initProgressCallback: (progress: { progress: number; text: string }) => {
        onProgress?.(progress);
      },
    });
    const loadTime = performance.now() - startTime;
    if (this.ldClient) {
      this.ldClient.track("performance", { type: "metric", context: "webllm_init", value: loadTime });
    }
  }

  async generateQuery(userPrompt: string, preferredTable: string = "sessions", contextQueries: PinnedItem[] = []): Promise<{
    query: QueryResponse;
    data: Record<string, unknown>[];
    metrics: QueryGenerationMetrics;
  }> {
    const startTime = Date.now();
    const metrics: QueryGenerationMetrics = {
      attemptCount: 0,
      validationUsed: false,
      initialValid: false,
      finalValid: false,
      totalTimeMs: 0
    };

    let shouldValidate = this.useValidation;
    if (this.ldClient) {
      const strategy = this.ldClient.variation("query-validation-strategy", "adaptive") as string;
      if (strategy === "strict") shouldValidate = true;
      else if (strategy === "fast") shouldValidate = false;
    }

    const validator = createQueryValidator(preferredTable);
    let sql = "";
    let table = preferredTable;
    let data: Record<string, unknown>[] = [];

    while (metrics.attemptCount < 3) {
      metrics.attemptCount++;
      
      if (metrics.attemptCount === 1) {
        const result = await this.generateSQLFromPrompt(userPrompt, preferredTable, contextQueries);
        sql = result.sql;
        table = result.table;

        // Auto-fix ORDER BY
        try {
          const parsed = this.parseQueryForChart(sql);
          const analyzer = createQueryAnalyzer(table);
          const analysis = analyzer.analyze({
            dimensions: parsed.dimensions.map(d => d.alias),
            measures: parsed.measures.map(m => m.alias)
          });
          if (analysis.dimensionCategories.temporal.length > 0 && !sql.toUpperCase().includes("ORDER BY")) {
            sql += ` ORDER BY ${analysis.dimensionCategories.temporal[0]} ASC`;
          }
        } catch (e) { console.warn(e); }
      }

      const validation = validator.validate(sql);
      if (metrics.attemptCount === 1) metrics.initialValid = validation.valid;
      
      if (!validation.valid && shouldValidate) {
        if (metrics.attemptCount >= 3) throw new Error("Validation failed after 3 attempts");
        const retryPrompt = `${validation.correctionPrompt}\n\nORIGINAL REQUEST: "${userPrompt}"`;
        const retryResult = await this.generateSQLFromPrompt(retryPrompt, table, contextQueries);
        sql = retryResult.sql;
        continue;
      }
      
      try {
        const result = await (this.semanticTables[table].db as any).query(sql);
        data = result.toArray().map((row: any) => row.toJSON());
        metrics.finalValid = true;
        const parsed = this.parseQueryForChart(sql);
        
        const querySpec: QueryResponse = {
          table,
          dimensions: parsed.dimensions,
          measures: parsed.measures,
          filters: parsed.filters,
          explanation: `Analyzing ${parsed.measures.map(m => m.alias).join(', ')}`,
          sql
        };
        metrics.totalTimeMs = Date.now() - startTime;
        return { query: querySpec, data, metrics };
      } catch (error) {
        metrics.executionError = (error as Error).message;
        if (metrics.attemptCount >= 3) throw error;
      }
    }
    throw new Error("Query generation failed");
  }

  private async generateSQLFromPrompt(
    prompt: string,
    preferredTable: string = "sessions",
    contextQueries: PinnedItem[] = []
  ): Promise<{ sql: string; table: string }> {
    const contextPrompt = generateSQLPrompt(preferredTable);
    
    let historyContext = "";
    if (contextQueries && contextQueries.length > 0) {
      historyContext = "\n### 📈 RELEVANT ANALYTICAL HISTORY (SUCCESSFUL PATTERNS):\n" + 
        contextQueries.slice(0, 5).map((q) => `- Goal: "${q.prompt}"\n  SQL: ${q.sql}`).join("\n");
    }

    const fullPrompt = `${contextPrompt}${historyContext}

User request: "${prompt}"

Return a JSON object: {"sql": "...", "explanation": "..."}`;

    const completion = await this.engine.chat.completions.create({
      messages: [
        { role: "system", content: "You are a specialized SQL Generator. Return ONLY valid JSON." },
        { role: "user", content: fullPrompt }
      ],
      temperature: 0.0,
      max_tokens: this.maxTokens,
    });

    const rawResponse = completion.choices[0].message.content.trim();
    let sql = "";
    try {
      const jsonMatch = rawResponse.match(/```(?:json)?\s*(\{[\s\S]*\})\s*```/i) || [null, rawResponse];
      const parsed = JSON.parse(jsonMatch[1] || rawResponse);
      sql = parsed.sql || "";
    } catch {
      const selectMatch = rawResponse.match(/SELECT\s+[\s\S]+?(?:;|$|(?="))/i);
      sql = selectMatch ? selectMatch[0] : "";
    }

    if (!sql) throw new Error("No SQL generated");
    return { sql: sql.replace(/;$/, '').trim(), table: preferredTable };
  }

  private parseQueryForChart(sql: string) {
    const dimensions: any[] = [];
    const measures: any[] = [];
    const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/is);
    if (selectMatch) {
      const cols = selectMatch[1].split(/,(?![^(]*\))/);
      cols.forEach(col => {
        const aliasMatch = col.match(/\s+[Aa][Ss]\s+["']?(\w+)["']?$/);
        const alias = aliasMatch ? aliasMatch[1] : col.trim();
        if (/^(SUM|AVG|COUNT|MAX|MIN)\s*\(/i.test(col.trim())) {
          measures.push({ alias, sourceField: alias });
        } else {
          dimensions.push({ alias, sourceField: alias });
        }
      });
    }
    return { dimensions, measures, filters: [] };
  }

  async chat(prompt: string): Promise<string> {
    const completion = await this.engine.chat.completions.create({
      messages: [{ role: "user", content: prompt }],
      temperature: 0.3, max_tokens: 500
    });
    return completion.choices[0].message.content;
  }
}
