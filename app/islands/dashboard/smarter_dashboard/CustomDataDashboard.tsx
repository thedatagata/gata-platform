// islands/dashboard/smarter_dashboard/CustomDataDashboard.tsx
import { useEffect, useState } from "preact/hooks";
import {
  createPlatformAPIClient,
  type ModelSummary,
  type ModelDetail,
  type MeasureDef,
  type AskResponse,
} from "../../../utils/api/platform-api-client.ts";
import FreshChartsWrapper from "../../../components/charts/FreshChartsWrapper.tsx";
import type { ChartConfig } from "../../../utils/smarter/autovisualization_dashboard/chart-generator.ts";
import { generateDashboardChartConfig } from "../../../utils/smarter/autovisualization_dashboard/dashboard-chart-generator.ts";
import { PinnedItem, loadPinnedItems, savePinnedItem, deletePinnedItem } from "../../../utils/smarter/dashboard_utils/query-persistence.ts";
import type { PoPMetric } from "../../../utils/smarter/autovisualization_dashboard/webllm-handler.ts";

interface CustomDataDashboardProps {
  webllmEngine: unknown | null;
  webllmReady: boolean;
  backendLLMAvailable: boolean;
  backendLLMProvider: string;
  tenantSlug: string;
  models: ModelSummary[];
  onBack?: () => void;
  onShowObservability?: () => void;
}

// Conversation history entry
interface ConversationEntry {
  role: "user" | "assistant";
  prompt?: string;
  answer?: string;
  records?: Record<string, unknown>[];
  chart?: ChartConfig | null;
  chartSpec?: Record<string, unknown> | null;
  insights?: string;
  modelUsed?: string;
  provider?: string;
  error?: string;
}

export default function CustomDataDashboard({
  webllmEngine,
  webllmReady,
  backendLLMAvailable,
  backendLLMProvider,
  tenantSlug,
  models,
  onBack,
  onShowObservability,
}: CustomDataDashboardProps) {
  // Model selection — prefer a model with measures for initial PoP cards
  const defaultModel = models.find(m => m.measure_count > 0)?.name || models[0]?.name || "";
  const [selectedModelName, setSelectedModelName] = useState<string>(defaultModel);
  const [modelDetail, setModelDetail] = useState<ModelDetail | null>(null);
  const [modelDetails, setModelDetails] = useState<Map<string, ModelDetail>>(new Map());
  const [modelLoading, setModelLoading] = useState(true);

  // PoP KPI state
  const [popMetrics, setPopMetrics] = useState<PoPMetric[]>([]);
  const [popLoading, setPopLoading] = useState(false);
  const [kpiInsights, setKpiInsights] = useState("");

  // Chat state
  const [prompt, setPrompt] = useState("");
  const [isProcessing, setIsProcessing] = useState(false);
  const [loadingStatus, setLoadingStatus] = useState("");

  // Results state
  const [chartConfig, setChartConfig] = useState<ChartConfig | null>(null);
  const [echartsSpec, setEchartsSpec] = useState<Record<string, unknown> | null>(null);
  const [backendAnswer, setBackendAnswer] = useState("");
  const [insights, setInsights] = useState("");
  const [lastProvider, setLastProvider] = useState("");
  const [lastError, setLastError] = useState<string | null>(null);

  // Pinned items
  const [pinnedItems, setPinnedItems] = useState<PinnedItem[]>([]);

  // Conversation history
  const [history, setHistory] = useState<ConversationEntry[]>([]);

  // Both LLMs required for conversational analytics
  const chatEnabled = webllmReady && backendLLMAvailable;

  // Compute chat status message
  const chatStatusMessage = !webllmReady && !backendLLMAvailable
    ? `WebLLM loading... Backend LLM unavailable${backendLLMProvider ? ` (${backendLLMProvider})` : ""}`
    : !webllmReady
      ? "Waiting for WebLLM to load..."
      : !backendLLMAvailable
        ? `Backend LLM unavailable${backendLLMProvider ? ` (${backendLLMProvider})` : ""}`
        : "";

  // Fetch model detail when selection changes
  useEffect(() => {
    if (!selectedModelName || !tenantSlug) return;

    let cancelled = false;
    setModelLoading(true);
    setModelDetail(null);
    setPopMetrics([]);
    setKpiInsights("");

    const client = createPlatformAPIClient();
    client.getModelDetail(tenantSlug, selectedModelName).then(detail => {
      if (!cancelled) {
        setModelDetail(detail);
        setModelDetails(prev => new Map(prev).set(selectedModelName, detail));
        setModelLoading(false);
      }
    }).catch(err => {
      if (!cancelled) {
        console.error("Failed to load model detail:", err);
        setModelLoading(false);
      }
    });

    // Load pinned items for this model
    loadPinnedItems(selectedModelName).then(items => {
      if (!cancelled) setPinnedItems(items);
    });

    return () => { cancelled = true; };
  }, [selectedModelName, tenantSlug]);

  // Prefetch all model details for context enrichment
  useEffect(() => {
    if (!tenantSlug || models.length === 0) return;
    const client = createPlatformAPIClient();
    for (const model of models) {
      if (!modelDetails.has(model.name)) {
        client.getModelDetail(tenantSlug, model.name).then(detail => {
          setModelDetails(prev => new Map(prev).set(model.name, detail));
        }).catch(() => { /* non-critical */ });
      }
    }
  }, [tenantSlug, models]);

  // Fetch PoP KPI metrics when model detail loads
  useEffect(() => {
    if (!modelDetail || !tenantSlug) return;

    let cancelled = false;
    setPopLoading(true);
    fetchPoPMetrics(modelDetail, tenantSlug).then(metrics => {
      if (!cancelled) {
        setPopMetrics(metrics);
        setPopLoading(false);

        // Generate proactive KPI insights via WebLLM (non-blocking)
        if (webllmReady && webllmEngine && metrics.length > 0) {
          const handler = webllmEngine as {
            analyzeKPICards: (metrics: PoPMetric[], modelName: string) => Promise<string>;
          };
          handler.analyzeKPICards(metrics, selectedModelName).then(result => {
            if (!cancelled) setKpiInsights(result);
          }).catch(() => { /* non-critical */ });
        }
      }
    }).catch(() => {
      if (!cancelled) setPopLoading(false);
    });

    return () => { cancelled = true; };
  }, [modelDetail, tenantSlug]);

  // Re-analyze KPI cards when WebLLM becomes available after PoP loaded
  useEffect(() => {
    if (!webllmReady || !webllmEngine || popMetrics.length === 0 || kpiInsights) return;
    const handler = webllmEngine as {
      analyzeKPICards: (metrics: PoPMetric[], modelName: string) => Promise<string>;
    };
    handler.analyzeKPICards(popMetrics, selectedModelName).then(result => {
      setKpiInsights(result);
    }).catch(() => { /* non-critical */ });
  }, [webllmReady, popMetrics]);

  // New query handler: WebLLM context enricher -> Backend /ask -> WebLLM insights
  const handleQuerySubmit = async () => {
    if (!prompt.trim() || !modelDetail) return;

    // Guard: both LLMs must be available
    if (!chatEnabled) {
      setLastError(chatStatusMessage);
      return;
    }

    setIsProcessing(true);
    setLastError(null);
    setChartConfig(null);
    setEchartsSpec(null);
    setBackendAnswer("");
    setInsights("");
    setLastProvider("");

    const userEntry: ConversationEntry = { role: "user", prompt: prompt.trim() };

    try {
      const handler = webllmEngine as {
        generateSemanticContext: (
          prompt: string,
          allModels: ModelSummary[],
          modelDetails: Map<string, ModelDetail>,
          contextQueries?: PinnedItem[],
        ) => Promise<string>;
        generateInsights: (
          prompt: string,
          data: Record<string, unknown>[],
        ) => Promise<string>;
      };

      // Pass 1: WebLLM generates semantic context (model/field hints)
      setLoadingStatus("Analyzing question...");
      const semanticContext = await handler.generateSemanticContext(
        prompt,
        models,
        modelDetails,
        pinnedItems,
      );

      // Pass 2: Backend /ask with question + semantic context
      setLoadingStatus("Querying data warehouse...");
      const client = createPlatformAPIClient();
      const askResponse: AskResponse = await client.askQuestion(tenantSlug, {
        question: prompt,
        semantic_context: semanticContext,
      });

      setBackendAnswer(askResponse.answer);
      setLastProvider(askResponse.provider);

      const records = askResponse.records || [];
      let resultChart: ChartConfig | null = null;

      // Use ECharts spec from BSL if available — convert to ChartConfig for Chart.js
      if (askResponse.chart_spec) {
        setEchartsSpec(askResponse.chart_spec);
        const converted = echartsSpecToChartConfig(askResponse.chart_spec);
        if (converted) {
          resultChart = converted;
          setChartConfig(converted);
        }
      }
      if (!resultChart && records.length > 1) {
        // Generate chart from records
        const columns = Object.keys(records[0]);
        const dims = columns.filter(c => typeof records[0][c] === "string");
        const measures = columns.filter(c => typeof records[0][c] === "number");
        if (dims.length > 0 && measures.length > 0) {
          resultChart = generateDashboardChartConfig(
            {
              dimensions: dims.slice(0, 2),
              measures: measures.slice(0, 3),
              chartType: "bar",
              title: `${measures[0]} Analysis`,
            },
            records,
          );
          if (resultChart) setChartConfig(resultChart);
        }
      }

      // Pass 3: WebLLM generates rich insights connecting results to KPI context
      let generatedInsights = "";
      if (records.length > 0) {
        setLoadingStatus("Generating insights...");
        try {
          generatedInsights = await handler.generateInsights(prompt, records);
          setInsights(generatedInsights);
        } catch (e) {
          console.warn("Insight generation failed:", e);
        }
      }

      setHistory(prev => [...prev, userEntry, {
        role: "assistant",
        answer: askResponse.answer,
        records,
        chart: resultChart,
        chartSpec: askResponse.chart_spec,
        insights: generatedInsights,
        modelUsed: askResponse.model_used,
        provider: askResponse.provider,
      }]);

    } catch (error: unknown) {
      console.error("Query execution failed:", error);
      const errMsg = error instanceof Error ? error.message : String(error);
      setLastError(errMsg);
      setHistory(prev => [...prev, userEntry, { role: "assistant", error: errMsg }]);
    } finally {
      setIsProcessing(false);
      setLoadingStatus("");
      setPrompt("");
    }
  };

  const handlePin = async () => {
    if (!chartConfig) return;
    const newItem: PinnedItem = {
      id: Math.random().toString(36).substr(2, 9),
      tableName: selectedModelName,
      config: chartConfig,
      explanation: insights || backendAnswer || "Pinned analysis",
      sql: "",
      prompt: history.filter(h => h.role === "user").pop()?.prompt || "",
      timestamp: Date.now(),
    };
    await savePinnedItem(newItem);
    setPinnedItems([newItem, ...pinnedItems]);
  };

  const handleModelSwitch = (modelName: string) => {
    setSelectedModelName(modelName);
    setChartConfig(null);
    setEchartsSpec(null);
    setBackendAnswer("");
    setInsights("");
    setKpiInsights("");
    setLastError(null);
    setHistory([]);
    setPrompt("");
    setLastProvider("");
  };

  return (
    <div class="min-h-screen bg-gata-darker">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-6">

        {/* Header Bar */}
        <div class="flex items-center justify-between bg-gata-dark/40 backdrop-blur-md p-6 rounded-2xl border border-gata-green/20 shadow-xl">
          <div class="flex items-center gap-6">
            {onBack && (
              <button
                type="button"
                onClick={onBack}
                class="text-gata-green hover:text-gata-hover font-bold text-xs uppercase tracking-widest transition-colors"
              >
                ← Back
              </button>
            )}
            <div>
              <h1 class="text-2xl font-black text-gata-cream italic tracking-tighter uppercase">
                Analytical Canvas
              </h1>
              <p class="text-[10px] text-gata-cream/40 font-medium uppercase tracking-[0.2em] mt-0.5">
                AI-powered data exploration
              </p>
            </div>
          </div>

          <div class="flex items-center gap-4">
            {/* LLM Status Indicators */}
            <div class="flex items-center gap-2">
              <span class={`w-2 h-2 rounded-full ${webllmReady ? "bg-green-400" : "bg-yellow-400 animate-pulse"}`} />
              <span class="text-[9px] text-gata-cream/30 font-bold uppercase tracking-widest">WebLLM</span>
              <span class={`w-2 h-2 rounded-full ml-2 ${backendLLMAvailable ? "bg-green-400" : "bg-red-400"}`} />
              <span class="text-[9px] text-gata-cream/30 font-bold uppercase tracking-widest">
                {backendLLMProvider || "Backend"}
              </span>
            </div>

            <select
              value={selectedModelName}
              onChange={(e) => handleModelSwitch((e.target as HTMLSelectElement).value)}
              class="bg-gata-dark border border-gata-green/20 text-gata-cream text-sm font-bold rounded-xl px-4 py-2.5 outline-none focus:border-gata-green transition-all"
            >
              {models.map(m => (
                <option key={m.name} value={m.name}>{m.label}</option>
              ))}
            </select>

            {onShowObservability && (
              <button
                type="button"
                onClick={onShowObservability}
                class="px-5 py-2.5 rounded-xl text-xs font-black uppercase tracking-widest transition-all bg-gata-dark/60 text-gata-cream/40 border border-gata-green/20 hover:text-gata-green"
              >
                Pipeline Health
              </button>
            )}
          </div>
        </div>

        {/* PoP KPI Cards */}
        {popMetrics.length > 0 && (
          <div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {popMetrics.map(metric => (
              <PoPCard key={metric.name} metric={metric} />
            ))}
          </div>
        )}
        {popLoading && (
          <div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {[1, 2, 3, 4].map(i => (
              <div key={i} class="bg-gata-dark/40 rounded-xl border border-gata-green/10 p-5 animate-pulse">
                <div class="h-3 bg-gata-green/10 rounded w-1/2 mb-3" />
                <div class="h-7 bg-gata-green/5 rounded w-3/4 mb-2" />
                <div class="h-3 bg-gata-green/5 rounded w-1/3" />
              </div>
            ))}
          </div>
        )}

        {/* KPI Insights (from WebLLM) */}
        {kpiInsights && (
          <div class="bg-gata-green/5 border border-gata-green/15 rounded-xl p-5">
            <h4 class="text-[10px] font-black text-gata-green uppercase tracking-widest mb-2">KPI Insights</h4>
            <p class="text-sm text-gata-cream/80 leading-relaxed whitespace-pre-wrap">{kpiInsights}</p>
          </div>
        )}

        <div class="flex flex-col lg:flex-row gap-6">
          {/* Left Panel: Semantic Context */}
          <div class="lg:w-80 flex-shrink-0 space-y-4">
            <SemanticContextPanel
              modelDetail={modelDetail}
              loading={modelLoading}
              selectedModelName={selectedModelName}
            />
          </div>

          {/* Right Panel: Chat + Results */}
          <div class="flex-1 flex flex-col gap-6 min-w-0">

            {/* Results Canvas */}
            <div class="bg-gata-dark/40 rounded-2xl border border-gata-green/10 min-h-[420px] flex flex-col">
              {isProcessing ? (
                <div class="flex flex-col items-center justify-center flex-1 py-20">
                  <div class="relative mb-6">
                    <div class="w-16 h-16 border-4 border-gata-green/30 rounded-full" />
                    <div class="w-16 h-16 border-4 border-gata-green rounded-full border-t-transparent animate-spin absolute top-0" />
                  </div>
                  <p class="text-gata-green font-bold text-sm animate-pulse uppercase tracking-widest">{loadingStatus}</p>
                </div>
              ) : (
                <div class="p-6 flex flex-col flex-1">
                  {/* Backend Answer */}
                  {backendAnswer && (
                    <div class="bg-gata-dark/60 border border-gata-green/10 rounded-xl p-5 mb-6">
                      <div class="flex items-center justify-between mb-2">
                        <h4 class="text-[10px] font-black text-gata-green uppercase tracking-widest">Backend Analysis</h4>
                        {lastProvider && (
                          <span class="text-[9px] text-gata-cream/20 font-mono">{lastProvider}</span>
                        )}
                      </div>
                      <p class="text-sm text-gata-cream/80 leading-relaxed whitespace-pre-wrap">{backendAnswer}</p>
                    </div>
                  )}

                  {/* Chart Result (from BSL ECharts or generated) */}
                  {(chartConfig || echartsSpec) && (
                    <div class="mb-6">
                      <div class="flex justify-between items-center mb-4">
                        <h4 class="text-[10px] font-black text-gata-green uppercase tracking-widest">Visualization</h4>
                        {chartConfig && (
                          <button
                            type="button"
                            onClick={handlePin}
                            class="px-4 py-2 bg-gata-green text-gata-dark font-black rounded-lg text-[10px] uppercase tracking-widest hover:bg-gata-hover transition-all"
                          >
                            Pin
                          </button>
                        )}
                      </div>
                      <div class="bg-gata-darker/60 rounded-xl p-4 border border-gata-green/5">
                        {chartConfig
                          ? <FreshChartsWrapper config={chartConfig} height={320} />
                          : echartsSpec && (
                            <div class="text-gata-cream/40 text-xs p-4 bg-gata-dark/40 rounded-lg text-center">
                              <p>Chart data received but could not be rendered.</p>
                            </div>
                          )
                        }
                      </div>
                    </div>
                  )}

                  {/* AI Insights (from WebLLM post-analysis) */}
                  {insights && (
                    <div class="bg-gata-green/5 border border-gata-green/15 rounded-xl p-5 mb-6">
                      <h4 class="text-[10px] font-black text-gata-green uppercase tracking-widest mb-2">Enriched Insights</h4>
                      <p class="text-sm text-gata-cream/80 leading-relaxed whitespace-pre-wrap">{insights}</p>
                    </div>
                  )}

                  {/* Error Display */}
                  {lastError && (
                    <div class="bg-red-900/20 border border-red-500/30 rounded-xl p-4 mb-6">
                      <p class="text-red-400 font-mono text-xs">{lastError}</p>
                    </div>
                  )}

                  {/* Empty State */}
                  {!chartConfig && !echartsSpec && !backendAnswer && !insights && !lastError && !isProcessing && (
                    <div class="flex flex-col items-center justify-center flex-1 py-16 text-center">
                      <div class="text-5xl opacity-5 mb-6 font-black text-gata-cream">?</div>
                      <p class="text-gata-cream/30 text-sm font-medium max-w-md">
                        {chatEnabled
                          ? "Ask a question about your data below. The AI will analyze your question, query the warehouse, and visualize the results."
                          : chatStatusMessage || "Waiting for AI services to become available..."}
                      </p>
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* Chat Input */}
            <div class="flex gap-3">
              <div class="flex-1 relative">
                <input
                  type="text"
                  value={prompt}
                  onInput={(e) => setPrompt((e.target as HTMLInputElement).value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      handleQuerySubmit();
                    }
                  }}
                  placeholder={!modelDetail
                    ? "Loading model definitions..."
                    : !chatEnabled
                      ? chatStatusMessage
                      : `e.g., Show me ${modelDetail.measures[0]?.name || "total revenue"} by ${modelDetail.dimensions.find(d => d.type === "string")?.name || "source"} over the last 30 days...`}
                  class="w-full bg-gata-dark border-2 border-gata-green/15 rounded-xl px-6 py-4 text-gata-cream font-medium focus:border-gata-green outline-none transition-all placeholder:text-gata-cream/15"
                  disabled={isProcessing || !modelDetail || !chatEnabled}
                />
                {/* Status bar under input when LLMs not ready */}
                {!chatEnabled && modelDetail && (
                  <p class="absolute -bottom-5 left-2 text-[9px] text-yellow-400/70 font-bold uppercase tracking-widest">
                    {chatStatusMessage}
                  </p>
                )}
              </div>
              <button
                type="button"
                onClick={handleQuerySubmit}
                disabled={isProcessing || !prompt.trim() || !modelDetail || !chatEnabled}
                class={`px-8 py-4 rounded-xl font-black text-xs uppercase tracking-widest transition-all ${
                  !isProcessing && prompt.trim() && modelDetail && chatEnabled
                    ? "bg-gata-green text-gata-dark hover:bg-gata-hover shadow-lg"
                    : "bg-gata-dark/40 text-gata-cream/20 cursor-not-allowed border border-gata-green/10"
                }`}
              >
                {isProcessing ? "Analyzing..." : !chatEnabled ? "Unavailable" : "Analyze"}
              </button>
            </div>

            {/* Pinned Items */}
            {pinnedItems.length > 0 && (
              <div class="space-y-4">
                <h3 class="text-[10px] font-black text-gata-green uppercase tracking-[0.3em]">Pinned Discoveries</h3>
                <div class="grid grid-cols-1 xl:grid-cols-2 gap-4">
                  {pinnedItems.map(item => (
                    <div key={item.id} class="bg-gata-dark/40 rounded-xl border border-gata-green/10 p-5 space-y-3 group relative hover:border-gata-green/20 transition-all">
                      <div class="flex justify-between items-start">
                        <p class="text-sm font-bold text-gata-cream italic leading-tight pr-8">{item.explanation}</p>
                        <button
                          type="button"
                          onClick={async () => {
                            await deletePinnedItem(item.id, selectedModelName);
                            setPinnedItems(pinnedItems.filter(p => p.id !== item.id));
                          }}
                          class="text-red-400 opacity-0 group-hover:opacity-100 transition-all font-black text-[9px] uppercase tracking-widest bg-red-400/10 px-2 py-1 rounded-full"
                        >
                          Remove
                        </button>
                      </div>
                      <div class="bg-gata-darker/50 rounded-lg p-3 border border-gata-green/5">
                        <FreshChartsWrapper config={item.config} height={220} />
                      </div>
                      <div class="flex justify-between text-[9px] text-gata-cream/20 font-bold uppercase tracking-widest">
                        <span>{item.prompt || "Analysis"}</span>
                        <span>{new Date(item.timestamp).toLocaleDateString()}</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}


// --- PoP KPI Card ---

function PoPCard({ metric }: { metric: PoPMetric }) {
  const isPositive = metric.changePercent > 0;
  const isNegative = metric.changePercent < 0;

  return (
    <div class="bg-gata-dark/40 rounded-xl border border-gata-green/10 p-5 hover:border-gata-green/20 transition-all">
      <p class="text-[10px] text-gata-cream/40 font-black uppercase tracking-widest mb-1 truncate">
        {metric.label}
      </p>
      <p class="text-2xl font-black text-gata-cream tracking-tight">
        {metric.format === "currency" ? "$" : ""}{formatCompact(metric.current)}
        {metric.format === "percentage" ? "%" : ""}
      </p>
      <div class="flex items-center gap-2 mt-1.5">
        <span class={`text-xs font-bold ${
          isPositive ? "text-green-400" : isNegative ? "text-red-400" : "text-gata-cream/30"
        }`}>
          {isPositive ? "+" : ""}{metric.changePercent.toFixed(1)}%
        </span>
        <span class="text-[10px] text-gata-cream/20">
          vs prev. {formatCompact(metric.previous)}
        </span>
      </div>
    </div>
  );
}

function formatCompact(val: number): string {
  const absVal = Math.abs(val);
  const sign = val < 0 ? "-" : "";
  if (absVal >= 1_000_000_000) return sign + (absVal / 1_000_000_000).toFixed(1) + "B";
  if (absVal >= 1_000_000) return sign + (absVal / 1_000_000).toFixed(1) + "M";
  if (absVal >= 1_000) return sign + (absVal / 1_000).toFixed(1) + "K";
  if (absVal === 0) return "0";
  if (absVal < 1) return sign + absVal.toFixed(2);
  return sign + absVal.toFixed(absVal < 100 ? 1 : 0);
}


// --- PoP Data Fetcher ---

async function fetchPoPMetrics(
  modelDetail: ModelDetail,
  tenantSlug: string,
): Promise<PoPMetric[]> {
  // Find date dimension
  const dateDim = modelDetail.dimensions.find(
    d => d.type === "date" || d.type === "timestamp" || d.type === "timestamp_epoch"
  );
  if (!dateDim) return [];

  // Pick top 4 aggregatable measures
  const topMeasures = modelDetail.measures.slice(0, 4);
  if (topMeasures.length === 0) return [];

  const measureNames = topMeasures.map(m => m.name);

  // Calculate date boundaries (30-day periods)
  const now = new Date();
  const thirtyDaysAgo = new Date(now);
  thirtyDaysAgo.setDate(now.getDate() - 30);
  const sixtyDaysAgo = new Date(now);
  sixtyDaysAgo.setDate(now.getDate() - 60);

  const isEpoch = dateDim.type === "timestamp_epoch";
  // DuckDB stores epoch timestamps in microseconds
  const toFilterValue = (d: Date): string | number =>
    isEpoch ? d.getTime() * 1000 : d.toISOString().split("T")[0];

  const client = createPlatformAPIClient();

  try {
    // Current period query (last 30 days)
    const currentQuery = {
      model: modelDetail.name,
      dimensions: [] as string[],
      measures: measureNames,
      calculated_measures: [] as string[],
      filters: [
        { field: dateDim.name, op: ">=", value: toFilterValue(thirtyDaysAgo) },
      ],
      joins: [] as string[],
      order_by: [] as Array<{ field: string; dir: "asc" | "desc" }>,
      limit: null as number | null,
    };

    // Previous period query (60-30 days ago)
    const previousQuery = {
      ...currentQuery,
      filters: [
        { field: dateDim.name, op: ">=", value: toFilterValue(sixtyDaysAgo) },
        { field: dateDim.name, op: "<", value: toFilterValue(thirtyDaysAgo) },
      ],
    };

    const [currentResult, previousResult] = await Promise.all([
      client.executeQuery(tenantSlug, currentQuery),
      client.executeQuery(tenantSlug, previousQuery),
    ]);

    const currentRow = currentResult.data[0] || {};
    const previousRow = previousResult.data[0] || {};

    return topMeasures.map((m: MeasureDef) => {
      const cur = Number(currentRow[m.name] || 0);
      const prev = Number(previousRow[m.name] || 0);
      const pct = prev === 0 ? (cur > 0 ? 100 : 0) : ((cur - prev) / prev) * 100;

      // Infer format from measure name
      let format: "number" | "currency" | "percentage" = "number";
      const nameLower = m.name.toLowerCase();
      if (nameLower.includes("revenue") || nameLower.includes("spend") || nameLower.includes("price") || nameLower.includes("cost")) {
        format = "currency";
      } else if (nameLower.includes("rate") || nameLower.includes("percent")) {
        format = "percentage";
      }

      return {
        name: m.name,
        label: m.label || m.name.replace(/_/g, " "),
        current: cur,
        previous: prev,
        changePercent: pct,
        format,
      };
    });
  } catch (err) {
    console.error("PoP fetch failed:", err);
    return [];
  }
}


// --- ECharts → ChartConfig converter ---

function echartsSpecToChartConfig(spec: Record<string, unknown>): ChartConfig | null {
  try {
    // deno-lint-ignore no-explicit-any
    const xAxisData: string[] = (spec.xAxis as any)?.data || [];
    // deno-lint-ignore no-explicit-any
    const series: any[] = (spec.series as any[]) || [];
    // deno-lint-ignore no-explicit-any
    const titleText: string = (spec.title as any)?.text || "Analysis";

    if (xAxisData.length === 0 || series.length === 0) return null;

    // Map ECharts type → ChartConfig type
    const eType = series[0]?.type || "bar";
    let chartType: ChartConfig["type"] = "bar";
    if (eType === "line") chartType = "line";
    else if (eType === "scatter") chartType = "line";

    const xKey = "category";
    const yKeys = series.map(
      // deno-lint-ignore no-explicit-any
      (s: any, idx: number) => s.name || `series_${idx}`,
    );

    // Build row-per-label data array
    // deno-lint-ignore no-explicit-any
    const data: Record<string, any>[] = xAxisData.map((label, i) => {
      // deno-lint-ignore no-explicit-any
      const row: Record<string, any> = { [xKey]: label };
      // deno-lint-ignore no-explicit-any
      for (const s of series) {
        const key = s.name || `series_${series.indexOf(s)}`;
        row[key] = s.data?.[i] ?? 0;
      }
      return row;
    });

    // Infer value format from measure names
    // deno-lint-ignore no-explicit-any
    const format: Record<string, any> = {};
    for (const key of yKeys) {
      const lower = key.toLowerCase();
      if (
        lower.includes("revenue") || lower.includes("spend") ||
        lower.includes("cost") || lower.includes("price")
      ) {
        format[key] = { type: "currency", decimals: 2 };
      } else if (
        lower.includes("rate") || lower.includes("percent") ||
        lower.includes("ctr") || lower.includes("cpc")
      ) {
        format[key] = { type: "percentage", decimals: 1 };
      } else {
        format[key] = { type: "number", decimals: 0 };
      }
    }

    return {
      type: chartType,
      title: titleText,
      xKey,
      yKeys,
      data,
      config: {
        showLegend: yKeys.length > 1,
        showGrid: true,
        showTooltip: true,
        format,
      },
    };
  } catch (e) {
    console.error("ECharts → ChartConfig conversion failed:", e);
    return null;
  }
}


// --- Semantic Context Panel ---

function SemanticContextPanel({
  modelDetail,
  loading,
  selectedModelName,
}: {
  modelDetail: ModelDetail | null;
  loading: boolean;
  selectedModelName: string;
}) {
  const [expandedSection, setExpandedSection] = useState<string | null>("measures");

  if (loading || !modelDetail) {
    return (
      <div class="bg-gata-dark/40 rounded-2xl border border-gata-green/10 p-6">
        <div class="animate-pulse space-y-4">
          <div class="h-4 bg-gata-green/10 rounded w-3/4" />
          <div class="h-3 bg-gata-green/5 rounded w-1/2" />
          <div class="space-y-2 mt-6">
            {[1, 2, 3, 4, 5].map(i => (
              <div key={i} class="h-6 bg-gata-green/5 rounded" />
            ))}
          </div>
        </div>
      </div>
    );
  }

  const sections = [
    {
      key: "measures",
      label: "Measures",
      count: modelDetail.measures.length + modelDetail.calculated_measures.length,
      color: "text-blue-300 bg-blue-900/40",
      items: [
        ...modelDetail.measures.map(m => ({
          name: m.label || m.name,
          detail: `${m.agg}(${m.name})`,
        })),
        ...modelDetail.calculated_measures.map(cm => ({
          name: cm.label || cm.name,
          detail: cm.sql,
        })),
      ],
    },
    {
      key: "dimensions",
      label: "Dimensions",
      count: modelDetail.dimensions.length,
      color: "text-green-300 bg-green-900/40",
      items: modelDetail.dimensions.map(d => ({
        name: d.name,
        detail: d.type,
      })),
    },
    {
      key: "joins",
      label: "Relationships",
      count: modelDetail.joins.length,
      color: "text-purple-300 bg-purple-900/40",
      items: modelDetail.joins.map(j => ({
        name: `-> ${j.to}`,
        detail: `${j.type} join on ${Object.entries(j.on).map(([k, v]) => `${k}=${v}`).join(", ")}`,
      })),
    },
  ];

  return (
    <div class="bg-gata-dark/40 rounded-2xl border border-gata-green/10 overflow-hidden">
      <div class="p-5 border-b border-gata-green/10">
        <h3 class="text-sm font-black text-gata-cream uppercase tracking-tight">{modelDetail.label || selectedModelName}</h3>
        <p class="text-[10px] text-gata-cream/40 mt-1 leading-relaxed">{modelDetail.description}</p>
      </div>

      {sections.map(section => (
        <div key={section.key} class="border-b border-gata-green/5 last:border-b-0">
          <button
            type="button"
            onClick={() => setExpandedSection(expandedSection === section.key ? null : section.key)}
            class="w-full flex items-center justify-between px-5 py-3 hover:bg-gata-green/5 transition-colors"
          >
            <div class="flex items-center gap-2">
              <span class={`px-2 py-0.5 rounded text-[9px] font-black uppercase ${section.color}`}>
                {section.label}
              </span>
              <span class="text-[10px] text-gata-cream/30 font-bold">{section.count}</span>
            </div>
            <svg
              class={`w-3 h-3 text-gata-cream/30 transition-transform ${expandedSection === section.key ? "rotate-180" : ""}`}
              fill="none" stroke="currentColor" viewBox="0 0 24 24"
            >
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
            </svg>
          </button>

          {expandedSection === section.key && (
            <div class="px-5 pb-4 space-y-1">
              {section.items.length === 0 ? (
                <p class="text-[10px] text-gata-cream/20 italic py-2">None configured</p>
              ) : (
                section.items.map((item, i) => (
                  <div key={i} class="flex items-start justify-between py-1.5">
                    <div class="min-w-0">
                      <p class="text-xs font-bold text-gata-cream truncate">{item.name}</p>
                      <p class="text-[10px] text-gata-cream/30 font-mono truncate">{item.detail}</p>
                    </div>
                  </div>
                ))
              )}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
