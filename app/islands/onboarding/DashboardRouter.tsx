// islands/onboarding/DashboardRouter.tsx
import { useState, useEffect, useRef, useCallback } from "preact/hooks";
import SmartDashLoadingPage from "../dashboard/smarter_dashboard/SmartDashLoadingPage.tsx";
import { SemanticLayer } from "../../utils/system/semantic-profiler.ts";
import CustomDataDashboard from "../dashboard/smarter_dashboard/CustomDataDashboard.tsx";
import DashboardSeeder, { SeedingInfo } from "../../islands/onboarding/DashboardSeeder.tsx";
import { registerCustomMetadata, getSemanticMetadata, type SemanticMetadata } from "../../utils/smarter/dashboard_utils/semantic-config.ts";
import { createPlatformAPIClient, type ModelSummary, type ReadinessStatus } from "../../utils/api/platform-api-client.ts";
import { adaptBSLModelToSemanticMetadata } from "../../utils/api/bsl-config-adapter.ts";
import PipelineHealthDashboard from "../dashboard/observability/PipelineHealthDashboard.tsx";
import { showToast } from "../app_utils/Toast.tsx";

interface DashboardRouterProps {
  motherDuckToken: string;
  sessionId: string;
  tenantSlug?: string;
}

export default function DashboardRouter({ motherDuckToken, sessionId, tenantSlug }: DashboardRouterProps) {
  const [smartDashInitialized, setSmartDashInitialized] = useState(false);
  // deno-lint-ignore no-explicit-any
  const [db, setDb] = useState<any>(null);
  // deno-lint-ignore no-explicit-any
  const [webllmEngine, setWebllmEngine] = useState<any>(null);

  const [activeTable, setActiveTable] = useState<string | null>(null);
  const [activeConfig, setActiveConfig] = useState<SemanticLayer | SemanticMetadata | null>(null);
  const [seedingInfo, setSeedingInfo] = useState<SeedingInfo | null>(null);

  // Connected mode state
  const [availableModels, setAvailableModels] = useState<ModelSummary[]>([]);
  const [connectedLoading, setConnectedLoading] = useState(false);
  const [connectedError, setConnectedError] = useState<string | null>(null);
  const [showObservability, setShowObservability] = useState(false);

  // Provisioning state (pipeline still building)
  const [isProvisioning, setIsProvisioning] = useState(false);
  const [provisioningStatus, setProvisioningStatus] = useState<ReadinessStatus | null>(null);

  const handleSmartDashReady = (dbConnection: unknown, engine: unknown) => {
    setDb(dbConnection);
    setWebllmEngine(engine);
    setSmartDashInitialized(true);
  };

  // Provisioning readiness polling (hooks must be before any early returns)
  const pollRef = useRef<number | null>(null);
  const delayRef = useRef(2000);

  const retryModelLoad = useCallback(() => {
    setIsProvisioning(false);
    setProvisioningStatus(null);
    setConnectedLoading(false);
    setConnectedError(null);
    setAvailableModels([]);
    delayRef.current = 2000;
  }, []);

  useEffect(() => {
    if (!isProvisioning || !tenantSlug) return;

    const client = createPlatformAPIClient();

    const poll = async () => {
      try {
        const status = await client.checkReadiness(tenantSlug);
        setProvisioningStatus(status);

        if (status.is_ready) {
          retryModelLoad();
          return;
        }
      } catch (e) {
        console.error("Provisioning poll error:", e);
      }
      delayRef.current = Math.min(delayRef.current * 1.5, 15000);
      pollRef.current = setTimeout(poll, delayRef.current) as unknown as number;
    };

    poll();
    return () => {
      if (pollRef.current) clearTimeout(pollRef.current);
    };
  }, [isProvisioning, tenantSlug, retryModelLoad]);

  // No tenant configured — show message
  if (!tenantSlug) {
    return (
      <div class="min-h-screen flex items-center justify-center bg-gata-dark">
        <div class="text-center space-y-6 max-w-md">
          <h2 class="text-2xl font-black text-gata-cream uppercase tracking-tight">No Tenant Configured</h2>
          <p class="text-gata-cream/40 text-sm leading-relaxed">
            Complete onboarding to connect your data sources and start exploring your analytics.
          </p>
          <a href="/" class="inline-block px-8 py-3 bg-gata-green text-gata-dark rounded-xl font-black uppercase tracking-widest text-xs hover:bg-[#a0d147] transition-all">
            Back to Home
          </a>
        </div>
      </div>
    );
  }

  // Observability view
  if (showObservability) {
    return (
      <PipelineHealthDashboard
        tenantSlug={tenantSlug}
        onBack={() => setShowObservability(false)}
      />
    );
  }

  // Model selection (before DuckDB/WebLLM initialization)
  if (!activeTable) {
    if (availableModels.length === 0 && !connectedLoading && !connectedError && !isProvisioning) {
      setConnectedLoading(true);
      const client = createPlatformAPIClient();
      client.getModels(tenantSlug).then(models => {
        setAvailableModels(models);
        setConnectedLoading(false);
        setConnectedError(null);
      }).catch(err => {
        console.error("Failed to load models:", err);
        const msg = (err as Error).message || "";
        setConnectedLoading(false);
        // Detect pipeline-not-ready vs actual backend failure
        if (msg.includes("No star schema tables") || msg.includes("Run dbt first") || msg.includes("bsl_column_catalog")) {
          setIsProvisioning(true);
        } else {
          setConnectedError(msg || "Failed to connect to analytics backend");
        }
      });
    }

    // Provisioning animation — pipeline is building star schema
    if (isProvisioning) {
      const statusLabel = provisioningStatus?.status || "starting";
      const statusMessages: Record<string, { label: string; progress: number }> = {
        starting:   { label: "Generating mock data...", progress: 10 },
        ingesting:  { label: "Ingesting raw data...", progress: 25 },
        modeling:   { label: "Building star schema...", progress: 50 },
        cataloging: { label: "Indexing semantic layer...", progress: 75 },
        error:      { label: "Pipeline error — retrying...", progress: 0 },
      };
      const { label: stepLabel, progress } = statusMessages[statusLabel] || statusMessages.starting;

      return (
        <div class="min-h-screen flex items-center justify-center p-4 bg-gradient-to-br from-gata-dark to-[#186018]">
          <div class="max-w-md w-full text-center space-y-8">
            <div>
              <h4 class="text-[10px] font-black text-gata-green uppercase tracking-[0.4em] mb-4">Provisioning</h4>
              <h2 class="text-3xl font-black text-gata-cream italic tracking-tighter uppercase">Building Your Analytics</h2>
              <p class="text-gata-cream/40 font-medium mt-3 text-sm">
                Your data pipeline is being configured. This typically takes 2-3 minutes.
              </p>
            </div>

            <div class="bg-gata-dark/80 backdrop-blur-sm rounded-2xl shadow-xl p-8 border border-gata-green/20">
              <div class="flex justify-center mb-6">
                <div class="relative">
                  <div class="w-20 h-20 border-4 border-gata-green/30 rounded-full" />
                  <div class="w-20 h-20 border-4 border-gata-green rounded-full border-t-transparent animate-spin absolute top-0" />
                </div>
              </div>

              <p class="text-lg font-medium text-gata-cream mb-1">{stepLabel}</p>
              <p class="text-sm text-gata-cream/50 mb-4">
                {provisioningStatus?.message || "Waiting for pipeline..."}
              </p>

              <div class="mb-2">
                <div class="flex justify-between text-xs text-gata-cream/70 mb-2">
                  <span>Progress</span>
                  <span>{progress}%</span>
                </div>
                <div class="w-full bg-gata-dark/60 rounded-full h-3 overflow-hidden border border-gata-green/30">
                  <div
                    class="h-3 bg-gradient-to-r from-gata-green to-[#a0d147] rounded-full transition-all duration-700 ease-out"
                    style={{ width: `${progress}%` }}
                  />
                </div>
              </div>
            </div>

            <a
              href="/"
              class="inline-block px-8 py-3 border border-gata-green/20 text-gata-cream/60 rounded-xl font-bold uppercase tracking-widest text-xs hover:text-gata-green hover:border-gata-green transition-all"
            >
              Back to Home
            </a>
          </div>
        </div>
      );
    }

    if (connectedError) {
      return (
        <div class="min-h-screen flex items-center justify-center p-4 bg-gata-dark">
          <div class="max-w-lg w-full text-center space-y-8">
            <div class="w-20 h-20 mx-auto rounded-full border-2 border-red-400/30 flex items-center justify-center">
              <svg class="w-10 h-10 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
              </svg>
            </div>
            <div>
              <h2 class="text-2xl font-black text-gata-cream uppercase tracking-tight mb-2">Backend Unavailable</h2>
              <p class="text-gata-cream/40 text-sm leading-relaxed">{connectedError}</p>
            </div>
            <div class="flex flex-col gap-4">
              <button
                type="button"
                onClick={() => { setConnectedError(null); setConnectedLoading(false); }}
                class="px-8 py-3 bg-gata-green text-gata-dark rounded-xl font-black uppercase tracking-widest text-xs hover:bg-[#a0d147] transition-all"
              >
                Retry Connection
              </button>
              <a
                href="/"
                class="px-8 py-3 border border-gata-green/20 text-gata-cream/60 rounded-xl font-bold uppercase tracking-widest text-xs hover:text-gata-green hover:border-gata-green transition-all text-center"
              >
                Back to Home
              </a>
            </div>
          </div>
        </div>
      );
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
                  try {
                    const client = createPlatformAPIClient();
                    const detail = await client.getModelDetail(tenantSlug, model.name);
                    const metadata = adaptBSLModelToSemanticMetadata(detail);
                    registerCustomMetadata(metadata);
                    setActiveTable(model.name);
                    setActiveConfig(metadata);
                  } catch (err) {
                    showToast(`Failed to load ${model.label}`, 'error');
                    setConnectedError(`Failed to load ${model.label}: ${(err as Error).message}`);
                  }
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
          </div>
        </div>
      </div>
    );
  }

  // Initialize DuckDB & WebLLM
  if (!smartDashInitialized) {
    return (
      <SmartDashLoadingPage
        onComplete={handleSmartDashReady}
        motherDuckToken={motherDuckToken}
        mode="connected"
        tenantSlug={tenantSlug}
      />
    );
  }

  // Baseline seeding
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

  // Dashboard workspace
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
            onShowObservability={() => setShowObservability(true)}
            tenantSlug={tenantSlug}
          />
        </div>
      </div>
    );
  }

  return <div class="min-h-screen bg-gata-dark flex items-center justify-center text-gata-cream">UNKNOWN STATE</div>;
}
