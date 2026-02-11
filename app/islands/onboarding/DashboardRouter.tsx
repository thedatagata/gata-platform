// islands/onboarding/DashboardRouter.tsx
import { useState, useEffect } from "preact/hooks";
import SmartDashLoadingPage from "../dashboard/smarter_dashboard/SmartDashLoadingPage.tsx";
import FileUploader from "../app_utils/FileUploader.tsx";
import SemanticProfiler from "../app_utils/SemanticProfiler.tsx";
import { SemanticLayer } from "../../utils/system/semantic-profiler.ts";
import { getLDClient, initializeLaunchDarkly } from "../../utils/launchdarkly/client.ts";
import ExperienceGateway from "../../islands/onboarding/ExperienceGateway.tsx";
import CustomDataDashboard from "../dashboard/smarter_dashboard/CustomDataDashboard.tsx";
import DashboardSeeder, { SeedingInfo } from "../../islands/onboarding/DashboardSeeder.tsx";
import { registerCustomMetadata, getSemanticMetadata, type SemanticMetadata } from "../../utils/smarter/dashboard_utils/semantic-config.ts";
import { SemanticReportObj } from "../../utils/smarter/dashboard_utils/semantic-objects.ts";
import { createPlatformAPIClient, type ModelSummary } from "../../utils/api/platform-api-client.ts";
import { adaptBSLModelToSemanticMetadata } from "../../utils/api/bsl-config-adapter.ts";
import PipelineHealthDashboard from "../dashboard/observability/PipelineHealthDashboard.tsx";

interface DashboardRouterProps {
  motherDuckToken: string;
  sessionId: string;
  ldClientId?: string;
  tenantSlug?: string;
}

export default function DashboardRouter({ motherDuckToken, sessionId, ldClientId, tenantSlug }: DashboardRouterProps) {
  const [smarterMode, setSmarterMode] = useState<'sample' | 'custom' | 'connected' | null>(null);
  const [smartDashInitialized, setSmartDashInitialized] = useState(false);
  // deno-lint-ignore no-explicit-any
  const [db, setDb] = useState<any>(null);
  // deno-lint-ignore no-explicit-any
  const [webllmEngine, setWebllmEngine] = useState<any>(null);

  const [sourceSelection, setSourceSelection] = useState<'upload' | null>(null);

  const [activeTable, setActiveTable] = useState<string | null>(null);
  const [activeConfig, setActiveConfig] = useState<SemanticLayer | SemanticMetadata | null>(null);
  const [seedingInfo, setSeedingInfo] = useState<SeedingInfo | null>(null);
  const [isProfiling, setIsProfiling] = useState(false);
  const [ldInitialized, setLdInitialized] = useState(!!getLDClient());

  // Connected mode state
  const [availableModels, setAvailableModels] = useState<ModelSummary[]>([]);
  const [connectedLoading, setConnectedLoading] = useState(false);
  const [showObservability, setShowObservability] = useState(false);

  useEffect(() => {
    const initLD = async () => {
      let client = getLDClient();
      if (!client && ldClientId) {
        const context = { kind: "user", key: sessionId, name: sessionId };
        client = await initializeLaunchDarkly(context, ldClientId);
      }
      if (client) setLdInitialized(true);
    };
    initLD();
  }, [ldClientId, sessionId]);

  const handleSmartDashReady = (dbConnection: unknown, engine: unknown) => {
    setDb(dbConnection);
    setWebllmEngine(engine);
    setSmartDashInitialized(true);
  };

  // 1. GATEWAY: Choose Sample vs Custom vs Connected
  if (!smarterMode) {
    return (
      <ExperienceGateway
        onSelect={(mode) => setSmarterMode(mode)}
        tenantSlug={tenantSlug}
      />
    );
  }

  // CONNECTED MODE: Observability view
  if (smarterMode === 'connected' && showObservability && tenantSlug) {
    return (
      <PipelineHealthDashboard
        tenantSlug={tenantSlug}
        onBack={() => setShowObservability(false)}
      />
    );
  }

  // CONNECTED MODE: Model selection (before DuckDB/WebLLM initialization)
  if (smarterMode === 'connected' && !activeTable && tenantSlug) {
    if (availableModels.length === 0 && !connectedLoading) {
      setConnectedLoading(true);
      const client = createPlatformAPIClient();
      client.getModels(tenantSlug).then(models => {
        setAvailableModels(models);
        setConnectedLoading(false);
      }).catch(err => {
        console.error("Failed to load models:", err);
        setConnectedLoading(false);
      });
    }

    if (connectedLoading) {
      return (
        <div class="min-h-screen flex items-center justify-center bg-gata-dark">
          <div class="text-center space-y-6">
            <div class="w-16 h-16 border-4 border-gata-green/30 border-t-gata-green rounded-full animate-spin mx-auto" />
            <p class="text-gata-green font-mono text-sm uppercase tracking-widest">Loading Star Schema Models...</p>
          </div>
        </div>
      );
    }

    return (
      <div class="min-h-screen flex items-center justify-center p-4 bg-gata-dark">
        <div class="max-w-4xl w-full text-center space-y-12">
          <div>
            <h4 class="text-[10px] font-black text-gata-green uppercase tracking-[0.4em] mb-4">Connected Analytics</h4>
            <h2 class="text-5xl font-black text-gata-cream italic tracking-tighter uppercase">Star Schema Models</h2>
            <p class="text-gata-cream/40 font-medium mt-4 text-sm">Select a model to explore</p>
          </div>
          <div class="grid grid-cols-2 md:grid-cols-3 gap-6">
            {availableModels.map(model => (
              <button
                type="button"
                key={model.name}
                onClick={async () => {
                  const client = createPlatformAPIClient();
                  const detail = await client.getModelDetail(tenantSlug!, model.name);
                  const metadata = adaptBSLModelToSemanticMetadata(detail);
                  registerCustomMetadata(metadata);
                  setActiveTable(model.name);
                  setActiveConfig(metadata);
                  setIsProfiling(false);
                }}
                class="bg-gata-dark border-2 border-gata-green/10 hover:border-gata-green hover:translate-y-[-4px] p-8 rounded-2xl transition-all text-left group"
              >
                <div class="text-lg font-black text-gata-cream uppercase tracking-tight group-hover:text-gata-green transition-colors">{model.label}</div>
                <div class="text-xs text-gata-cream/40 mt-2 leading-relaxed">{model.description}</div>
                <div class="flex gap-4 mt-4 text-xs text-gata-green/60">
                  <span>{model.dimension_count} dims</span>
                  <span>{model.measure_count} measures</span>
                  {model.has_joins && <span class="text-gata-green/80">+ joins</span>}
                </div>
              </button>
            ))}
          </div>
          <div class="flex items-center justify-center gap-8">
            <button
              type="button"
              onClick={() => setShowObservability(true)}
              class="border border-gata-green/30 text-gata-cream/60 hover:text-gata-green hover:border-gata-green px-6 py-3 rounded-xl text-xs transition-all uppercase font-bold tracking-widest"
            >
              Pipeline Health
            </button>
            <button type="button" onClick={() => setSmarterMode(null)} class="text-gata-cream/20 hover:text-gata-green text-xs transition-colors uppercase font-bold tracking-widest">
              ← Back
            </button>
          </div>
        </div>
      </div>
    );
  }

  // 2. LOADER: Initialize DuckDB & WebLLM (for sample/custom modes)
  if (!smartDashInitialized && smarterMode !== 'connected') {
    return <SmartDashLoadingPage onComplete={handleSmartDashReady} motherDuckToken={motherDuckToken} mode={smarterMode === 'connected' ? 'custom' : smarterMode} />;
  }

  if (!ldInitialized && smarterMode !== 'connected') {
    return <div class="min-h-screen flex items-center justify-center bg-gata-dark text-gata-green font-mono">INITIALIZING ANALYTICAL KERNEL...</div>;
  }

  // 3. SOURCE SELECTION / UPLOAD / CLOUD (sample/custom only)
  if (!activeTable) {
    if (smarterMode === 'custom') {

      if (sourceSelection === 'upload') {
        return (
          <div class="min-h-screen flex items-center justify-center p-4 bg-gata-dark">
            <div class="max-w-4xl w-full">
              <div class="bg-gata-dark border-2 border-gata-green/30 rounded-3xl p-12 shadow-2xl space-y-8">
                <div class="text-center">
                  <h2 class="text-5xl font-black text-gata-cream italic tracking-tighter mb-4">DRAG. DROP. <span class="text-gata-green">PROFIT.</span></h2>
                  <p class="text-gata-cream/40 font-medium">Upload any CSV, Parquet, or JSON to start your AI analysis.</p>
                </div>
                <FileUploader
                  db={db}
                  onUploadComplete={(tableName: string) => { setActiveTable(tableName); setIsProfiling(true); }}
                />
                <button type="button" onClick={() => setSourceSelection(null)} class="text-gata-cream/20 hover:text-gata-green text-xs w-full transition-colors uppercase font-bold tracking-widest">← Back to Selection</button>
              </div>
            </div>
          </div>
        );
      }

      return (
        <div class="min-h-screen flex items-center justify-center p-4 bg-gata-dark">
          <div class="max-w-4xl w-full text-center space-y-12">
            <h2 class="text-6xl font-black text-gata-cream italic tracking-tighter uppercase mb-4">Select Source</h2>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
              <button
                type="button"
                onClick={() => setSourceSelection('upload')}
                class="bg-gata-dark border-4 border-gata-green/10 hover:border-gata-green hover:translate-y-[-8px] p-12 rounded-3xl transition-all group"
              >
                <div class="text-5xl mb-6"></div>
                <div class="text-2xl font-black text-gata-cream uppercase tracking-tight">Local Upload</div>
                <p class="text-xs text-gata-cream/40 mt-4 font-medium italic">CSV, Parquet, JSON from your machine.</p>
              </button>
            </div>
            <button type="button" onClick={() => setSmarterMode(null)} class="text-gata-cream/20 hover:text-gata-green text-xs transition-colors uppercase font-bold tracking-widest">← Back to Gateway</button>
          </div>
        </div>
      );
    } else {
      // Sample selection
      return (
        <div class="min-h-screen flex items-center justify-center p-4 bg-gata-dark">
          <div class="max-w-3xl w-full text-center space-y-12">
            <h2 class="text-6xl font-black text-gata-cream italic tracking-tighter uppercase underline decoration-gata-green decoration-8 underline-offset-8">Select Dataset</h2>
            <div class="grid grid-cols-2 gap-8 px-4">
               <button type="button" onClick={() => { setActiveTable('session_facts'); setIsProfiling(true); }} class="bg-gata-dark border-4 border-gata-green/10 hover:border-gata-green hover:translate-y-[-8px] p-10 rounded-3xl transition-all group relative overflow-hidden">
                  <span class="text-5xl block mb-6 group-hover:scale-110 transition-transform"></span>
                  <span class="text-2xl font-black text-gata-cream uppercase tracking-tight">Product Analytics</span>
                  <p class="text-xs text-gata-cream/40 mt-4 leading-relaxed font-medium">Real-time session events, conversion funnels, and user pathing datasets.</p>
               </button>
               <button type="button" onClick={() => { setActiveTable('users_dim'); setIsProfiling(true); }} class="bg-gata-dark border-4 border-gata-green/10 hover:border-gata-green hover:translate-y-[-8px] p-10 rounded-3xl transition-all group relative overflow-hidden">
                  <span class="text-5xl block mb-6 group-hover:scale-110 transition-transform"></span>
                  <span class="text-2xl font-black text-gata-cream uppercase tracking-tight">Growth Data</span>
                  <p class="text-xs text-gata-cream/40 mt-4 leading-relaxed font-medium">User retention, engagement scoring, and demographic breakdown data.</p>
               </button>
            </div>
            <button type="button" onClick={() => setSmarterMode(null)} class="text-gata-cream/20 hover:text-gata-green text-xs transition-colors uppercase font-bold tracking-widest">← Back to Choice</button>
          </div>
        </div>
      );
    }
  }

  // 4. SEMANTIC PROFILING: Validate/Create Dimensions & Measures (sample/custom only)
  if (isProfiling && activeTable) {
    return (
      <div class="min-h-screen bg-gata-dark py-12 px-4 shadow-inner">
        <div class="max-w-6xl mx-auto">
          <SemanticProfiler
            db={db}
            tableName={activeTable}
            webllmEngine={webllmEngine}
            onComplete={(config: SemanticLayer) => {
              registerCustomMetadata(config);
              if (webllmEngine) {
                const engine = webllmEngine as { registerTable: (tableName: string, report: SemanticReportObj) => void };
                engine.registerTable(config.table, new SemanticReportObj(db, config.table));
              }
              setActiveConfig(config);
              setIsProfiling(false);
            }}
            onCancel={() => { setActiveTable(null); setIsProfiling(false); }}
          />
        </div>
      </div>
    );
  }

  // 5. BASELINE SEEDING
  if (!seedingInfo && activeTable && activeConfig) {
    return (
      <div class="min-h-screen flex items-center justify-center p-4 bg-gata-dark">
        <DashboardSeeder
          initialTableName={activeTable}
          onComplete={(seeding) => {
            if (seeding.table !== activeTable) {
              setActiveTable(seeding.table);
              setActiveConfig(getSemanticMetadata(seeding.table) as unknown as SemanticLayer);
            }
            setSeedingInfo(seeding);
          }}
          onBack={() => { setActiveTable(null); setActiveConfig(null); }}
        />
      </div>
    );
  }

  // 6. GENERALIZED SMARTER WORKSPACE
  if (seedingInfo && activeTable && activeConfig) {
    return (
      <div class="min-h-screen bg-gata-darker">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <CustomDataDashboard
            db={db}
            webllmEngine={webllmEngine}
            tableName={activeTable}
            semanticConfig={activeConfig}
            seedingInfo={seedingInfo}
            onBack={() => { setSeedingInfo(null); }}
            onShowObservability={smarterMode === 'connected' && tenantSlug ? () => setShowObservability(true) : undefined}
          />
        </div>
      </div>
    );
  }

  return <div class="min-h-screen bg-gata-dark flex items-center justify-center text-gata-cream">UNKNOWN STATE</div>;
}
