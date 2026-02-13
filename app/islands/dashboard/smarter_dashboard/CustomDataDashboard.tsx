// islands/dashboard/smarter_dashboard/CustomDataDashboard.tsx
import { useEffect, useState, useMemo } from "preact/hooks";
import { SemanticLayer, SemanticField } from "../../../utils/system/semantic-profiler.ts";
import { 
  registerCustomMetadata, 
  findMeasureByAlias, 
  type SemanticMetadata, 
  generateDataCatalog, 
  getSemanticMetadata, 
  sanitizeValue 
} from "../../../utils/smarter/dashboard_utils/semantic-config.ts";
import { createPlatformAPIClient } from "../../../utils/api/platform-api-client.ts";
import { SeedingInfo } from "../../onboarding/DashboardSeeder.tsx";
import FreshChartsWrapper from "../../../components/charts/FreshChartsWrapper.tsx";
import KPICard from "../../../components/charts/KPICard.tsx";
import type { ChartConfig } from "../../../utils/smarter/autovisualization_dashboard/chart-generator.ts";
import { generateDashboardChartConfig } from "../../../utils/smarter/autovisualization_dashboard/dashboard-chart-generator.ts";
import { PinnedItem, loadPinnedItems, savePinnedItem, deletePinnedItem } from "../../../utils/smarter/dashboard_utils/query-persistence.ts";
import { createSemanticReportObj } from "../../../utils/smarter/dashboard_utils/semantic-objects.ts";

interface CustomDataDashboardProps {
  db: unknown;
  webllmEngine: unknown;
  tableName: string;
  semanticConfig: SemanticLayer | SemanticMetadata;
  seedingInfo?: SeedingInfo;
  onBack: () => void;
  onShowObservability?: () => void;
  tenantSlug?: string;
}

export default function CustomDataDashboard({
  db,
  webllmEngine,
  tableName,
  semanticConfig,
  seedingInfo,
  onBack,
  onShowObservability,
  tenantSlug,
}: CustomDataDashboardProps) {
  const [loading, setLoading] = useState(true);
  const [showCatalog, setShowCatalog] = useState(false);
  const [pinnedItems, setPinnedItems] = useState<PinnedItem[]>([]);
  const [initialInsight, setInitialInsight] = useState<string>("");
  const [recommendations, setRecommendations] = useState<string[]>([]);
  
  // AI Exploration State
  const [promptInput, setPromptInput] = useState("");
  const [generatedSQL, setGeneratedSQL] = useState("");
  const [editedSQL, setEditedSQL] = useState("");
  const [sqlExplanation, setSqlExplanation] = useState("");
  const [sqlGenerating, setSqlGenerating] = useState(false);
  const [lastError, setLastError] = useState<string | null>(null);
  
  // Workshop State (restored)
  const [generatedChart, setGeneratedChart] = useState<ChartConfig | null>(null);
  const [kpiResult, setKpiResult] = useState<{value: number, title: string} | null>(null);

  // Phase 2: Automated Insights State
  const [breakdownChart, setBreakdownChart] = useState<ChartConfig | null>(null);
  const [trendsChart, setTrendsChart] = useState<ChartConfig | null>(null);
  const [initialAnalysisError, setInitialAnalysisError] = useState<string | null>(null);

  const catalog = useMemo(() => generateDataCatalog(tableName), [tableName]);
  const metadata = useMemo(() => getSemanticMetadata(tableName), [tableName]);

  useEffect(() => {
    registerCustomMetadata(semanticConfig);
    
    async function initializeDashboard() {
      console.log(" Initializing Dashboard with Automated Insights V2.5-SMART-TEMPORAL", { tableName, seedingInfo });
      // Load persisting items first
      const existingPins = await loadPinnedItems(tableName);
      setPinnedItems(existingPins);

      if (!webllmEngine || !seedingInfo || !db) {
        setLoading(false);
        return;
      }
      
      const primaryMeasure = seedingInfo.primaryMeasure;
      const dimension = seedingInfo.dimension;
      const dateField = seedingInfo.dateField;
      const timeRange = seedingInfo.timeRange;
      const filters = seedingInfo.filters || [];
      const days = timeRange.includes('day') ? parseInt(timeRange) : (timeRange.includes('month') ? parseInt(timeRange) * 30 : 30);
      
      try {
        const semanticTable = createSemanticReportObj(db, tableName);
        const database = db as { query: (sql: string) => Promise<{ toArray: () => Array<{ toJSON: () => Record<string, unknown> }> }> };
        
        // Helper for robust temporal casting
        const temporalCast = (col: string) => `COALESCE(TRY_CAST("${col}" AS TIMESTAMP), try_strptime(CAST("${col}" AS VARCHAR), '%m/%d/%Y'), try_strptime(CAST("${col}" AS VARCHAR), '%d/%m/%Y'), try_strptime(CAST("${col}" AS VARCHAR), '%Y/%m/%d'), try_strptime(CAST("${col}" AS VARCHAR), '%m-%d-%Y'))`;
        
        // 1. Fetch Reference Date (Max Date)
        const refResult = await database.query(`SELECT MAX(${temporalCast(dateField)}) as max_date FROM "${tableName}"`);
        const maxD = refResult.toArray()[0].toJSON().max_date as string | undefined;
        const refD = maxD ? `'${new Date(maxD).toISOString().split('T')[0]}'::DATE` : 'current_date';

        // Helper for filters
        const sqlFilters = [
          `${temporalCast(dateField)} >= ${refD} - INTERVAL '${days} days'`,
          ...filters.map((f: { field: string, value: string }) => `"${f.field}" = '${f.value}'`)
        ];

        // 2. Fetch Breakdown Data using Semantic Layer
        const brData = await semanticTable.query({
          dimensions: [dimension],
          measures: [primaryMeasure],
          filters: sqlFilters,
          orderBy: [`"${primaryMeasure}" DESC`],
          limit: 10
        });

        const brConfig = generateDashboardChartConfig(
          { table: tableName, dimensions: [dimension], measures: [primaryMeasure], title: `${primaryMeasure} by ${dimension}` },
          brData
        );
        setBreakdownChart(brConfig);

        // 3. Fetch Timeseries Trends using Semantic Layer
        const allMeasures = seedingInfo.measures;
        // Fetch trends by date.
        const trendsData = await semanticTable.query({
          dimensions: [dateField],
          measures: allMeasures,
          filters: sqlFilters,
          orderBy: [1]
        });

        const trConfig = generateDashboardChartConfig(
          { table: tableName, dimensions: [dateField], measures: allMeasures, title: `Daily Performance Trends` },
          trendsData
        );
        setTrendsChart(trConfig);

        // 4. Generate AI Analysis & Recommendations with Semantic Context
        const catalog = generateDataCatalog(tableName);
        const semanticContext = JSON.stringify({
          dimensions: catalog.dimensions.map(d => ({ alias: d.alias, description: d.description })),
          measures: catalog.measures.map(m => ({ alias: m.alias, description: m.description }))
        });

        const prompt = `You are an expert data analyst for "${tableName}".
        WORKSPACE VERSION: 2.2-SEMANTIC
        
        AVAILABLE SEMANTIC LAYER:
        ${semanticContext}
        
        ACTIVE ANALYTICAL CONTEXT:
        - Primary Metric: "${primaryMeasure}"
        - Main Dimension: "${dimension}"
        - Lookback: Last ${days} days
        
        DATA SAMPLES (JSON):
        - Breakdown by "${dimension}": ${JSON.stringify(brData.slice(0, 5))}
        - Recent Trends (Daily): ${JSON.stringify(trendsData.slice(-5))}
        
        TASK:
        1. Summarize the performance of "${primaryMeasure}" across "${dimension}".
        2. Identify any significant spikes, drops, or concentration of value in the trend/breakdown.
        3. Suggest 3 specific "drilled-down" AI queries the user could run next using the AVAILABLE SEMANTIC LAYER aliases.
        
        Format as:
        SUMMARY: [summary]
        REC1: [rec1]
        REC2: [rec2]
        REC3: [rec3]`;

        const engine = webllmEngine as { chat: (prompt: string) => Promise<string> };
        const response = await engine.chat(prompt);
        const lines = response.split('\n');
        const summary = lines.find((l: string) => l.startsWith('SUMMARY:'))?.replace('SUMMARY:', '').trim();
        const recs = lines.filter((l: string) => l.startsWith('REC')).map((l: string) => l.replace(/REC\d:/, '').trim());
        
        setInitialInsight(summary || "Dashboard prepared with semantic-aware automated insights!");
        setRecommendations(recs);
      } catch (e) {
        console.error("Initial analysis failed:", e);
        setInitialAnalysisError(e instanceof Error ? e.message : String(e));
        setInitialInsight("Workspace ready. All systems operational.");
      } finally {
        setLoading(false);
      }
    }

    initializeDashboard();
  }, [tableName, semanticConfig, webllmEngine, seedingInfo, db]);

  // ... inside CustomDataDashboard component ...

  const handleGenerateSQL = async () => {
    if (!promptInput.trim() || !webllmEngine) return;
    setSqlGenerating(true);
    setLastError(null);
    setGeneratedChart(null);
    setKpiResult(null);
    setGeneratedSQL(""); // Clear previous
    setSqlExplanation("");

    try {
      // --- HYBRID FLOW: CONNECTED MODE ---
      // If we have a tenantSlug, we utilize the Backend BSL Agent
      if ((window as any).tenantSlug || tenantSlug) { 
         const slug = tenantSlug || (window as any).tenantSlug;
         
         // PASS 1: Local LLM Planning (Text -> JSON)
         console.log("[Hybrid] Pass 1: Generating Semantic Request locally...");
         const handler = webllmEngine as { 
           generateSemanticRequest: (prompt: string, model: string, context?: PinnedItem[]) => Promise<any> 
         };
         
         const semanticRequest = await handler.generateSemanticRequest(promptInput, tableName, pinnedItems);
         console.log("[Hybrid] Pass 1 Result:", semanticRequest);
         setGeneratedSQL(JSON.stringify(semanticRequest, null, 2));
         setSqlExplanation("Generated Semantic Intent (BSL Protocol)");

         // PASS 2: Backend Execution (JSON -> SQL -> Data)
         console.log("[Hybrid] Pass 2: Executing via Platform API...");
         const client = createPlatformAPIClient();
         const result = await client.executeQuery(slug, semanticRequest);
         
         // Render Results
         const rows = result.data;
         
         // Heuristic for Chart Title
         const title = semanticRequest.measures?.[0] ? `${semanticRequest.measures[0]} Analysis` : 'Query Result';
         
         // KPI Check
         const isKPI = rows.length === 1 && Object.keys(rows[0]).length === 1;
         if (isKPI && rows.length > 0) {
             const key = Object.keys(rows[0])[0];
             setKpiResult({
               value: Number(rows[0][key]),
               title: key.replace(/_/g, ' ').toUpperCase()
             });
         } else if (rows.length > 0) {
             const chartConfig = generateDashboardChartConfig(
               {
                 dimensions: semanticRequest.dimensions || [],
                 measures: semanticRequest.measures || [],
                 chartType: 'bar', // default, generator will refine
                 title: title
               },
               rows
             );
             if (chartConfig) setGeneratedChart(chartConfig);
         }
         
      } else {
        // --- LOCAL MODE: DUCKDB WASM ---
        const handler = webllmEngine as { 
             generateQuery: (prompt: string, table: string, context?: PinnedItem[]) => Promise<{ query: { sql: string, explanation: string } }> 
        };
        const { query } = await handler.generateQuery(promptInput, tableName, pinnedItems);
           
        setGeneratedSQL(query.sql);
        setEditedSQL(query.sql);
        setSqlExplanation(query.explanation);
           
        // Auto-execute the generated query
        await executeSQL(query.sql, promptInput);
      }

    } catch (error: unknown) {
      console.error("AI Generation failed:", error);
      setLastError(error instanceof Error ? error.message : String(error));
    } finally {
      setSqlGenerating(false);
    }
  };

  const executeSQL = async (sql: string, title?: string) => {
    try {
      const database = db as { query: (sql: string) => Promise<{ toArray: () => Array<{ toJSON: () => Record<string, unknown> }> }> };
      const res = await database.query(sql);
      const rows = res.toArray().map(r => r.toJSON());
      const sanitizedData = rows.map(r => {
        const row: Record<string, unknown> = {};
        for (const k in r) row[k] = sanitizeValue(r[k]);
        return row;
      });

      // Try to parse query context for visualization if available
      const handler = webllmEngine as { parseQueryForChart?: (sql: string) => { dimensions: string[], measures: string[] } };
      let queryContext = null;
      if (handler.parseQueryForChart) {
        queryContext = handler.parseQueryForChart(sql);
      }

      const isKPI = sanitizedData.length === 1 && Object.keys(sanitizedData[0]).length === 1;
      
      if (isKPI && sanitizedData.length > 0) {
        const key = Object.keys(sanitizedData[0])[0];
        setKpiResult({
          value: Number(sanitizedData[0][key]),
          title: key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
        });
      } else if (sanitizedData.length > 0) {
        // Fallback chart generation
        const chartConfig = generateDashboardChartConfig(
          {
            dimensions: queryContext?.dimensions || [],
            measures: queryContext?.measures || [],
            chartType: 'bar',
            title: title || 'Query Result'
          },
          sanitizedData
        );
        if (chartConfig) setGeneratedChart(chartConfig);
      }
    } catch (error: unknown) {
      console.error("SQL Execution failed:", error);
      setLastError(error instanceof Error ? error.message : String(error));
    }
  };

  const handleManualExecute = async () => {
    // If it looks like JSON, we assume it's a semantic request for backend
    // If it looks like SQL, it's local
    if (!editedSQL.trim()) return;
    setSqlGenerating(true);
    setLastError(null);

    // Hybrid Check for Manual Override
    if (editedSQL.trim().startsWith('{') && (tenantSlug || (window as any).tenantSlug)) {
        const slug = tenantSlug || (window as any).tenantSlug;
        try {
            const req = JSON.parse(editedSQL);
            const client = createPlatformAPIClient();
            const result = await client.executeQuery(slug, req);
            // Render logic (duped for now, could refactor)
            const rows = result.data;
             const isKPI = rows.length === 1 && Object.keys(rows[0]).length === 1;
             if (isKPI && rows.length > 0) {
                 const key = Object.keys(rows[0])[0];
                 setKpiResult({ value: Number(rows[0][key]), title: key });
             } else if (rows.length > 0) {
                 const chartConfig = generateDashboardChartConfig({ dimensions: req.dimensions||[], measures: req.measures||[], chartType: 'bar', title: 'Manual Query' }, rows);
                 if (chartConfig) setGeneratedChart(chartConfig);
             }
        } catch (e) {
            setLastError("Invalid JSON for Backend Request or API Error");
        }
    } else {
        await executeSQL(editedSQL, "Custom Result");
    }
    setSqlGenerating(false);
  };

  const handlePin = async () => {
    if (!generatedChart) return;
    const newItem: PinnedItem = { 
      id: Math.random().toString(36).substr(2, 9), 
      tableName,
      config: generatedChart, 
      explanation: sqlExplanation || "AI Discovery", 
      sql: editedSQL, 
      prompt: promptInput,
      timestamp: Date.now() 
    };
    await savePinnedItem(newItem);
    setPinnedItems([newItem, ...pinnedItems]);
    // Reset workbench
    setGeneratedChart(null);
    setGeneratedSQL("");
    setEditedSQL("");
  };

  if (loading) {
    return (
      <div class="p-40 text-center font-mono text-gata-green animate-pulse tracking-[0.5em]">
        SYNCING ANALYTICAL WORKSPACE...
      </div>
    );
  }

  return (
    <div class="space-y-8 pb-32">
      {/* Refined Header */}
      <div class="flex items-center justify-between bg-gata-dark/40 backdrop-blur-md p-8 rounded-3xl border border-gata-green/20 shadow-xl">
        <div>
          <button
            type="button"
            onClick={onBack}
            class="text-gata-green hover:text-gata-hover font-bold text-xs uppercase tracking-widest mb-2 flex items-center gap-2 transition-colors"
          >
            ← Back to Origin
          </button>
          <h1 class="text-3xl font-black text-gata-cream italic tracking-tighter uppercase flex items-center gap-3">
             <span class="text-gata-green"></span> {tableName} Workspace
             <span class="ml-4 px-2 py-0.5 bg-gata-green text-gata-dark text-[10px] font-black rounded-full animate-pulse">V2.5-SMART-TEMPORAL</span>
          </h1>
          {metadata && <p class="text-xs text-gata-cream/40 font-medium uppercase tracking-[0.2em] mt-1">{metadata.description}</p>}
        </div>
        <div class="flex gap-4">
          {onShowObservability && (
            <button
              type="button"
              onClick={onShowObservability}
              class="px-6 py-3 rounded-xl text-xs font-black uppercase tracking-widest transition-all bg-gata-dark/60 text-gata-cream/40 border border-gata-green/20 hover:text-gata-green"
            >
              Pipeline Health
            </button>
          )}
          <button
            type="button"
            onClick={() => setShowCatalog(!showCatalog)}
            class={`px-6 py-3 rounded-xl text-xs font-black uppercase tracking-widest transition-all ${
              showCatalog ? 'bg-gata-green text-gata-dark' : 'bg-gata-dark/60 text-gata-cream/40 border border-gata-green/20 hover:text-gata-green'
            }`}
          >
            {showCatalog ? 'Hide' : 'Show'} Data Catalog
          </button>
        </div>
      </div>
      
      {initialAnalysisError && (
        <div class="px-8 py-4 bg-red-500/10 border border-red-500/30 rounded-2xl text-red-500 text-xs font-mono">
            Initial analytical sweep failed: {initialAnalysisError}
        </div>
      )}

      {/* Collapsible Data Catalog Table */}
      {showCatalog && metadata && (
        <div class="bg-gata-dark/60 border border-gata-green/30 rounded-3xl p-8 space-y-6 backdrop-blur-sm animate-in slide-in-from-top-4 duration-300">
          <div class="flex justify-between items-center">
            <h3 class="text-xl font-black text-gata-green italic uppercase">Table definition: <span class="text-gata-cream">{tableName}</span></h3>
            <span class="text-[10px] font-black text-gata-cream/30 uppercase tracking-[0.3em]">{Object.keys(metadata.fields).length} FIELDS REGISTERED</span>
          </div>
          <div class="bg-gata-dark/40 rounded-2xl border border-gata-green/10 overflow-hidden">
            <div class="overflow-x-auto max-h-[500px]">
              <table class="min-w-full text-left">
                <thead class="bg-gata-green/10 border-b border-gata-green/20 sticky top-0 backdrop-blur-md">
                  <tr>
                    <th class="px-6 py-4 text-[10px] font-black text-gata-green uppercase tracking-widest">Field</th>
                    <th class="px-6 py-4 text-[10px] font-black text-gata-green uppercase tracking-widest">Type</th>
                    <th class="px-6 py-4 text-[10px] font-black text-gata-green uppercase tracking-widest">Category</th>
                    <th class="px-6 py-4 text-[10px] font-black text-gata-green uppercase tracking-widest">Context</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-gata-green/5">
                  {Object.entries(metadata.fields).map(([fieldName, config]) => {
                    const field = config as SemanticField;
                    return (
                      <tr key={fieldName} class="hover:bg-gata-green/5 transition-colors">
                        <td class="px-6 py-4 font-mono text-xs text-gata-cream font-bold">{fieldName}</td>
                        <td class="px-6 py-4 text-[10px] text-gata-cream/60 font-medium">{field.md_data_type}</td>
                        <td class="px-6 py-4">
                          <span class={`inline-flex px-2 py-1 rounded-md text-[9px] font-black uppercase tracking-tighter ${
                            field.data_type_category === 'identifier' ? 'bg-purple-900/40 text-purple-300' :
                            field.data_type_category === 'temporal' ? 'bg-blue-900/40 text-blue-300' :
                            field.data_type_category === 'categorical' ? 'bg-green-900/40 text-green-300' :
                            'bg-yellow-900/40 text-yellow-300'
                          }`}>
                            {field.data_type_category}
                          </span>
                        </td>
                        <td class="px-6 py-4 text-xs text-gata-cream/80 italic">{field.description}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* KPI Row & Recommendations */}
      <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
        {seedingInfo && (
           <PoPCardWrapper 
             db={db}
             tableName={tableName}
             measure={seedingInfo.primaryMeasure}
             dateField={seedingInfo.dateField}
             timeRange={seedingInfo.timeRange}
             title={seedingInfo.primaryMeasure}
             filters={seedingInfo.filters || []}
           />
        )}
        
        <div class={`bg-gata-dark/40 ${seedingInfo ? 'md:col-span-2' : 'md:col-span-3'} p-8 rounded-3xl border border-gata-green/10 flex flex-col justify-center relative overflow-hidden group`}>
            <div class="absolute top-0 right-0 p-8 text-4xl opacity-5 grayscale group-hover:scale-110 transition-transform"></div>
            <p class="text-xl font-black text-gata-cream italic leading-tight mb-6">"{initialInsight}"</p>
            <div class="grid md:grid-cols-3 gap-4">
                {recommendations.map((rec, i) => (
                  <div key={i} class="p-3 bg-gata-green/5 border border-gata-green/10 rounded-2xl">
                    <h4 class="text-[9px] font-black text-gata-green uppercase tracking-widest mb-1">Tip #{i+1}</h4>
                    <p class="text-[10px] text-gata-cream/60 leading-tight italic">{rec}</p>
                  </div>
                ))}
            </div>
        </div>
      </div>

      {/* Automated Initial Insights */}
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {breakdownChart && (
          <div class="bg-gata-dark/40 p-8 rounded-[3rem] border border-gata-green/10 space-y-4">
            <h4 class="text-[10px] font-black text-gata-green uppercase tracking-widest pl-2">Automatic Breakdown</h4>
            <div class="bg-gata-darker/40 rounded-3xl p-6 border border-gata-green/5 shadow-inner">
              <FreshChartsWrapper config={breakdownChart} height={350} />
            </div>
          </div>
        )}
        {trendsChart && (
          <div class="bg-gata-dark/40 p-8 rounded-[3rem] border border-gata-green/10 space-y-4">
            <h4 class="text-[10px] font-black text-gata-green uppercase tracking-widest pl-2">Performance Trends</h4>
            <div class="bg-gata-darker/40 rounded-3xl p-6 border border-gata-green/5 shadow-inner">
              <FreshChartsWrapper config={trendsChart} height={350} />
            </div>
          </div>
        )}
      </div>

      {/* Pinned Insights Section */}
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {pinnedItems.map(item => (
          <div key={item.id} class="bg-gata-dark/60 rounded-[3rem] border border-gata-green/10 p-10 space-y-6 hover:border-gata-green/30 transition-all group relative backdrop-blur-sm">
             <div class="flex justify-between items-start">
                <h4 class="text-2xl font-black text-gata-cream italic leading-tight pr-12">{item.explanation}</h4>
                <button 
                  type="button" 
                  onClick={async () => {
                    await deletePinnedItem(item.id, tableName);
                    setPinnedItems(pinnedItems.filter(p => p.id !== item.id));
                  }} 
                  class="text-red-400 opacity-0 group-hover:opacity-100 transition-all font-black text-[10px] uppercase tracking-widest bg-red-400/10 px-3 py-1 rounded-full"
                >
                  Remove
                </button>
             </div>
             <div class="bg-gata-darker/50 rounded-2xl p-6 border border-gata-green/5 shadow-inner text-gata-cream">
               <FreshChartsWrapper config={item.config} height={350} />
             </div>
             <div class="flex justify-between items-center text-[10px] font-black text-gata-cream/20 uppercase tracking-widest">
                <span>Discovery: {item.prompt || 'Autopilot'}</span>
                <span>{new Date(item.timestamp).toLocaleDateString()}</span>
             </div>
          </div>
        ))}
      </div>

      {/* Semantic Layer Summary Table */}
      <div class="bg-gata-dark/40 border border-gata-green/10 rounded-3xl p-8 shadow-sm backdrop-blur-sm space-y-6">
        <h3 class="text-[10px] font-black text-gata-green uppercase tracking-[0.4em]"> Semantic Layer Architecture</h3>
        <div class="bg-gata-dark/40 rounded-2xl border border-gata-green/5 overflow-hidden">
          <div class="overflow-x-auto max-h-80">
            <table class="min-w-full text-left">
              <thead class="bg-gata-green/10 border-b border-gata-green/20 sticky top-0">
                <tr>
                  <th class="px-6 py-4 text-[10px] font-black text-gata-green uppercase tracking-widest">Entity Alias</th>
                  <th class="px-6 py-4 text-[10px] font-black text-gata-green uppercase tracking-widest">Role</th>
                  <th class="px-6 py-4 text-[10px] font-black text-gata-green uppercase tracking-widest">Logic Policy</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-gata-green/5">
                {catalog.dimensions.map(dim => (
                  <tr key={dim.alias} class="hover:bg-gata-green/5 transition-colors">
                    <td class="px-6 py-4 font-black text-sm text-gata-cream">{dim.alias}</td>
                    <td class="px-6 py-4"><span class="px-2 py-1 bg-green-900/40 text-green-300 text-[10px] font-black rounded uppercase">Dimension</span></td>
                    <td class="px-6 py-4 text-[10px] text-gata-cream/40 font-mono italic">{dim.description}</td>
                  </tr>
                ))}
                {catalog.measures.map(measure => (
                  <tr key={measure.alias} class="hover:bg-gata-green/5 transition-colors">
                    <td class="px-6 py-4 font-black text-sm text-gata-cream">{measure.alias}</td>
                    <td class="px-6 py-4"><span class="px-2 py-1 bg-blue-900/40 text-blue-300 text-[10px] font-black rounded uppercase">Measure</span></td>
                    <td class="px-6 py-4 text-[10px] text-gata-cream/40 font-mono italic">{measure.description}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* AI Exploration Section (Traditional Prompt Style) */}
      <div class="bg-gata-dark/60 border-2 border-gata-green/20 rounded-[3rem] p-10 shadow-2xl space-y-8 backdrop-blur-xl relative overflow-hidden group">
        <div class="absolute top-0 right-0 p-12 text-6xl opacity-10 group-hover:rotate-12 transition-transform"></div>
        <div>
           <h3 class="text-2xl font-black text-gata-cream italic uppercase tracking-tighter">AI Discovery <span class="text-gata-green">Analyis</span></h3>
           <p class="text-xs text-gata-cream/40 font-medium uppercase mt-1 tracking-widest">Interact with your personal data analyst in plain English</p>
        </div>
        
        <div class="space-y-4">
          <textarea
            value={promptInput}
            onInput={(e) => setPromptInput((e.target as HTMLInputElement).value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                handleGenerateSQL();
              }
            }}
            placeholder="e.g. 'Compare session count by country for the last 90 days'"
            class="w-full bg-gata-darker border-4 border-gata-green/10 rounded-[2rem] p-8 text-xl font-black text-gata-cream italic focus:border-gata-green outline-none transition-all placeholder:text-gata-cream/10 min-h-[120px]"
            disabled={sqlGenerating}
          />
          <div class="flex justify-end gap-3">
            <button
              type="button"
              onClick={handleGenerateSQL}
              disabled={!promptInput.trim() || sqlGenerating}
              class={`px-10 py-4 rounded-2xl font-black text-xs uppercase tracking-widest shadow-2xl transition-all ${
                promptInput.trim() && !sqlGenerating
                  ? "bg-gata-green text-gata-dark hover:bg-gata-hover"
                  : "bg-gata-dark/40 text-gata-cream/20 cursor-not-allowed border border-gata-green/10"
              }`}
            >
              {sqlGenerating ? "PROCESSING..." : "GENERATE SQL"}
            </button>
          </div>
        </div>

        {/* Workbench Results */}
        {generatedSQL && (
          <div class="space-y-8 animate-in zoom-in-95 duration-500">
            <div class="space-y-3">
               <label class="text-[10px] font-black text-purple-400 uppercase tracking-widest flex items-center gap-2">
                 <span class="w-2 h-2 bg-purple-500 rounded-full animate-pulse"></span> SQL WORKBENCH
               </label>
               <textarea
                 value={editedSQL}
                 onInput={(e) => setEditedSQL((e.target as HTMLInputElement).value)}
                 class="w-full bg-gata-dark border border-purple-500/30 rounded-2xl p-6 font-mono text-xs text-purple-300 outline-none focus:border-purple-500 transition-all min-h-[150px]"
               />
               <div class="flex gap-4">
                  <button
                    type="button"
                    onClick={handleManualExecute}
                    disabled={sqlGenerating}
                    class="flex-1 py-4 bg-purple-500 text-white font-black rounded-xl hover:bg-purple-600 transition-all uppercase tracking-widest text-xs"
                  >
                    RUN OVERRIDE
                  </button>
                  <button
                    type="button"
                    onClick={handleGenerateSQL}
                    class="px-8 py-4 bg-gata-dark/60 text-gata-cream/40 border border-gata-green/10 rounded-xl font-black uppercase tracking-widest text-xs hover:text-gata-green"
                  >
                    RE-GENERATE
                  </button>
               </div>
            </div>

            {sqlExplanation && (
              <div class="bg-gata-green/5 border border-gata-green/20 rounded-2xl p-6 italic">
                <p class="text-gata-cream/90 text-sm leading-relaxed pr-8">"{sqlExplanation}"</p>
              </div>
            )}

            {lastError && (
              <div class="bg-red-900/20 border-2 border-red-500/30 rounded-2xl p-6">
                <p class="text-red-400 font-mono text-xs">{lastError}</p>
              </div>
            )}

            {kpiResult && (
              <div class="animate-in fade-in slide-in-from-bottom-4 duration-700">
                <h4 class="text-[10px] font-black text-gata-green uppercase tracking-widest mb-4">Discovery Result</h4>
                <div class="max-w-sm">
                  <KPICard 
                    title={kpiResult.title}
                    value={kpiResult.value}
                    format={kpiResult.title.toLowerCase().includes('rate') ? 'percentage' : 
                          kpiResult.title.toLowerCase().includes('revenue') ? 'currency' : 'number'}
                    loading={false}
                  />
                </div>
              </div>
            )}

            {generatedChart && (
              <div class="animate-in fade-in slide-in-from-bottom-4 duration-700 space-y-6">
                <div class="flex justify-between items-center">
                   <h4 class="text-[10px] font-black text-gata-green uppercase tracking-widest">Discovery Visualization</h4>
                   <button
                     type="button"
                     onClick={handlePin}
                     class="px-6 py-3 bg-gata-green text-gata-dark font-black rounded-xl text-[10px] uppercase tracking-widest hover:bg-gata-hover transition-all"
                   >
                      PIN TO DASHBOARD
                   </button>
                </div>
                <div class="bg-gata-darker/60 rounded-[2rem] p-8 border border-gata-green/5 shadow-inner">
                  <FreshChartsWrapper config={generatedChart} height={400} loading={false} />
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

interface PoPCardWrapperProps {
  db: unknown;
  tableName: string;
  measure: string;
  dateField: string;
  timeRange: string;
  title: string;
  filters: Array<{ field: string; value: string }>;
}

// Internal Wrapper for PoPCard that translates Semantic logic to KPICard
function PoPCardWrapper({ db, tableName, measure, dateField, timeRange, title, filters }: PoPCardWrapperProps) {
  const [data, setData] = useState<{ cur: number, prev: number, pct: number } | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchPoP() {
      try {
        setLoading(true);
        const mDef = findMeasureByAlias(measure, tableName);
        const rCol = mDef?.sourceColumn || measure;
        const rAgg = mDef?.aggregationType || 'sum';
        
        const database = db as { query: (sql: string) => Promise<{ toArray: () => Array<{ toJSON: () => Record<string, unknown> }> }> };
        const temporalCast = (col: string) => `COALESCE(TRY_CAST("${col}" AS TIMESTAMP), try_strptime(CAST("${col}" AS VARCHAR), '%m/%d/%Y'), try_strptime(CAST("${col}" AS VARCHAR), '%d/%m/%Y'), try_strptime(CAST("${col}" AS VARCHAR), '%Y/%m/%d'), try_strptime(CAST("${col}" AS VARCHAR), '%m-%d-%Y'))`;
        const castDateField = temporalCast(dateField);
        
        // 1. Fetch Reference Date (Max Date)
        const refResult = await database.query(`SELECT MAX(${castDateField}) as max_date FROM "${tableName}"`);
        const maxD = refResult.toArray()[0].toJSON().max_date as string | undefined;
        const refD = maxD ? `'${new Date(maxD).toISOString().split('T')[0]}'::DATE` : 'current_date';
        
        const days = timeRange.includes('day') ? parseInt(timeRange) : (timeRange.includes('month') ? parseInt(timeRange) * 30 : 30);
        const fStr = filters.map((f: { field: string, value: string }) => `AND "${f.field}" = '${f.value}'`).join(" ");

        const query = `
          WITH stats AS (
            SELECT 
              ${rAgg.toUpperCase()}(CASE WHEN ${castDateField} >= ${refD} - INTERVAL '${days} days' THEN "${rCol}" ELSE 0 END) as cur,
              ${rAgg.toUpperCase()}(CASE WHEN ${castDateField} < ${refD} - INTERVAL '${days} days' AND ${castDateField} >= ${refD} - INTERVAL '${days * 2} days' THEN "${rCol}" ELSE 0 END) as prev
            FROM "${tableName}"
            WHERE ${castDateField} >= ${refD} - INTERVAL '${days * 2} days' ${fStr}
          ) SELECT * FROM stats`;
        
        const rs = await database.query(query);
        const row = rs.toArray()[0].toJSON();
        const cur = Number(sanitizeValue(row.cur) || 0);
        const prev = Number(sanitizeValue(row.prev) || 0);
        setData({ 
          cur, 
          prev, 
          pct: prev === 0 ? 0 : ((cur - prev) / prev) * 100 
        });
      } catch (e) { 
        console.error(e); 
      } finally { 
        setLoading(false); 
      }
    }
    fetchPoP();
  }, [db, tableName, measure, dateField, timeRange, filters]);

  return (
    <KPICard 
      title={title.replace(/_/g, ' ').toUpperCase()} 
      value={data?.cur || 0}
      previousValue={data?.prev}
      changePercent={data?.pct}
      format={title.toLowerCase().includes('revenue') ? 'currency' : 'number'}
      loading={loading}
    />
  );
}
