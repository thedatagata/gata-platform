// utils/smarter/autovisualization_dashboard/webllm-handler.ts
import { CreateMLCEngine } from "@mlc-ai/web-llm";
import type { ModelSummary, ModelDetail } from "../../../utils/api/platform-api-client.ts";
import type { PinnedItem } from "../dashboard_utils/query-persistence.ts";

// PoP metric type — used by KPI card analysis (standalone from conversational flow)
export interface PoPMetric {
  name: string;
  label: string;
  current: number;
  previous: number;
  changePercent: number;
  format: "number" | "currency" | "percentage";
}

export class WebLLMSemanticHandler {
  private engine: any;
  private modelId: string;
  private maxTokens: number;

  private static MODEL_TIERS = {
    "3b": "Qwen2.5-Coder-3B-Instruct-q4f16_1-MLC",
    "7b": "Qwen2.5-Coder-7B-Instruct-q4f16_1-MLC"
  };

  constructor(
    _semanticTables: Record<string, unknown> = {},
    tier: "3b" | "7b" = "3b",
  ) {
    this.modelId = WebLLMSemanticHandler.MODEL_TIERS[tier];
    this.maxTokens = 800;
  }

  async initialize(onProgress?: (progress: { progress: number; text: string }) => void) {
    const startTime = performance.now();
    this.engine = await CreateMLCEngine(this.modelId, {
      initProgressCallback: (progress: { progress: number; text: string }) => {
        onProgress?.(progress);
      },
    });
    const loadTime = performance.now() - startTime;
    console.log(`[WebLLM] Model loaded in ${Math.round(loadTime)}ms`);
  }

  /**
   * Pass 1: Generate semantic context string for the backend /ask endpoint.
   *
   * Instead of building a full SemanticQueryRequest JSON (which a 3B model
   * gets wrong), this produces a plain-text hint describing which models,
   * dimensions, and measures are likely relevant. The backend BSL agent
   * (with full schema awareness via BSLTools) uses this as a starting hint.
   */
  async generateSemanticContext(
    userPrompt: string,
    allModels: ModelSummary[],
    modelDetails: Map<string, ModelDetail>,
    contextQueries: PinnedItem[] = [],
  ): Promise<string> {
    // Build a compact catalog for the prompt — include joins and calculated measures
    const catalogLines: string[] = [];
    for (const model of allModels) {
      const detail = modelDetails.get(model.name);
      if (!detail) {
        catalogLines.push(`- ${model.name}: ${model.label} (${model.dimension_count} dims, ${model.measure_count} measures)`);
        continue;
      }
      const dims = detail.dimensions.map(d => `${d.name} (${d.type})`).join(", ");
      const measures = detail.measures.map(m => `${m.name} [${m.agg}]`).join(", ");
      const calcs = detail.calculated_measures.map(c => c.name).join(", ");
      const joins = detail.joins.map(j => `${j.to} (${j.type} on ${Object.entries(j.on).map(([k, v]) => `${k}=${v}`).join(", ")})`).join("; ");
      catalogLines.push(
        `MODEL: ${model.name} — ${detail.label}\n` +
        `  Dimensions: ${dims}\n` +
        `  Measures: ${measures}` +
        (calcs ? `\n  Calculated measures: ${calcs}` : "") +
        (joins ? `\n  Joins: ${joins}` : "")
      );
    }

    let historyHint = "";
    if (contextQueries.length > 0) {
      historyHint = "\n\nPrevious successful queries:\n" +
        contextQueries.slice(0, 3).map(q => `- "${q.prompt}"`).join("\n");
    }

    const systemPrompt = `You are a semantic context analyzer for a BSL (Boring Semantic Layer). Given a user's analytics question and a catalog of available data models, output structured candidate bindings that map directly to the BSL model structure.

Output this exact format using field names from the catalog:

RELEVANT_MODELS: <model_name>
CANDIDATE_DIMENSIONS:
  - <dim_name> (grouping)
  - <dim_name> (time axis)
  - <dim_name> (filter only)
CANDIDATE_MEASURES:
  - <measure_name> (aggregation: <agg>)
CANDIDATE_CALCULATED_MEASURES:
  - <calc_name>
SUGGESTED_JOINS:
  - <join_target_model> (reason)
TIME_FILTER: <range or "none">
GROUPING_STRATEGY: <brief description>

Rules:
- Use EXACT field names from the catalog
- Separate dimensions used for grouping vs filtering
- Include calculated measures when the question implies derived metrics (rates, ratios, averages)
- Suggest joins only when the question references fields from a related model
- Omit sections with no candidates`;

    const userContent = `Available models:
${catalogLines.join("\n")}
${historyHint}

User question: "${userPrompt}"

Candidate bindings:`;

    const completion = await this.engine.chat.completions.create({
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userContent },
      ],
      temperature: 0.1,
      max_tokens: this.maxTokens,
    });

    return completion.choices[0].message.content.trim();
  }

  /**
   * Pass 3: Generate plain-English insights from query results.
   * Analyzes the returned data and produces a summary + follow-up ideas.
   * This handles post-query insight generation for the chat flow only —
   * KPI/PoP analysis is handled separately by analyzeKPICards().
   */
  async generateInsights(
    originalPrompt: string,
    data: Record<string, unknown>[],
  ): Promise<string> {
    const sampleData = data.slice(0, 10);
    const columns = data.length > 0 ? Object.keys(data[0]) : [];

    const prompt = `You are a data analyst. The user asked: "${originalPrompt}"

Here are the query results (${data.length} total rows, showing first ${sampleData.length}):
Columns: ${columns.join(", ")}
Data: ${JSON.stringify(sampleData, null, 1)}

Provide:
1. A 2-3 sentence summary of the key findings
2. One notable pattern or outlier in the data
3. Two follow-up questions the user could explore next

Keep it concise and actionable. No markdown headers.`;

    const completion = await this.engine.chat.completions.create({
      messages: [
        { role: "system", content: "You are a concise data analyst. Provide clear insights from query results." },
        { role: "user", content: prompt }
      ],
      temperature: 0.3,
      max_tokens: 500,
    });

    return completion.choices[0].message.content.trim();
  }

  /**
   * Analyze PoP KPI cards and generate proactive insights + suggested prompts.
   * Called after KPI cards load to give the user conversation starters.
   */
  async analyzeKPICards(popMetrics: PoPMetric[], modelName: string): Promise<string> {
    if (popMetrics.length === 0) return "";

    const metricsText = popMetrics.map(m =>
      `- ${m.label}: ${m.current.toLocaleString()} (${m.changePercent >= 0 ? "+" : ""}${m.changePercent.toFixed(1)}% change)`
    ).join("\n");

    const prompt = `You are a data analyst looking at KPI cards for the "${modelName}" dataset.

Current metrics (last 30 days vs previous 30 days):
${metricsText}

Provide:
1. A 1-2 sentence overall assessment of performance
2. Highlight the most significant change and what it might mean
3. Suggest 2 specific questions the user could ask to dig deeper (phrase as natural language questions they can type)

Keep it brief and actionable. No markdown headers.`;

    const completion = await this.engine.chat.completions.create({
      messages: [
        { role: "system", content: "You are a concise data analyst providing proactive insights." },
        { role: "user", content: prompt }
      ],
      temperature: 0.3,
      max_tokens: 400,
    });

    return completion.choices[0].message.content.trim();
  }

}
