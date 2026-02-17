// islands/onboarding/DashboardRouter.tsx
import { useState, useEffect, useRef, useCallback } from "preact/hooks";
import CustomDataDashboard from "../dashboard/smarter_dashboard/CustomDataDashboard.tsx";
import { createPlatformAPIClient, type ModelSummary, type ReadinessStatus } from "../../utils/api/platform-api-client.ts";
import PipelineHealthDashboard from "../dashboard/observability/PipelineHealthDashboard.tsx";

interface DashboardRouterProps {
  sessionId: string;
  tenantSlug?: string;
}

export default function DashboardRouter({ tenantSlug }: DashboardRouterProps) {
  // WebLLM state — loads in background, dashboard is usable without it
  // deno-lint-ignore no-explicit-any
  const [webllmEngine, setWebllmEngine] = useState<any>(null);
  const [webllmReady, setWebllmReady] = useState(false);
  const [webllmLoading, setWebllmLoading] = useState(false);
  const [webllmProgress, setWebllmProgress] = useState(0);
  const [webllmStatus, setWebllmStatus] = useState("");

  // Backend LLM state
  const [backendLLMAvailable, setBackendLLMAvailable] = useState(false);
  const [backendLLMProvider, setBackendLLMProvider] = useState("");

  // Models
  const [availableModels, setAvailableModels] = useState<ModelSummary[]>([]);
  const [modelsLoading, setModelsLoading] = useState(false);
  const [modelsError, setModelsError] = useState<string | null>(null);
  const [showObservability, setShowObservability] = useState(false);

  // Provisioning state (pipeline still building)
  const [isProvisioning, setIsProvisioning] = useState(false);
  const [provisioningStatus, setProvisioningStatus] = useState<ReadinessStatus | null>(null);

  // Provisioning readiness polling
  const pollRef = useRef<number | null>(null);
  const delayRef = useRef(2000);

  const retryModelLoad = useCallback(() => {
    setIsProvisioning(false);
    setProvisioningStatus(null);
    setModelsLoading(false);
    setModelsError(null);
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

  // Check readiness first, then load models only when pipeline is done
  useEffect(() => {
    if (!tenantSlug || availableModels.length > 0 || modelsLoading || modelsError || isProvisioning) return;

    setModelsLoading(true);
    const client = createPlatformAPIClient();

    // Check readiness before attempting to load models
    client.checkReadiness(tenantSlug).then(status => {
      if (!status.is_ready) {
        // Pipeline still building — go straight to provisioning (no 404 errors)
        setModelsLoading(false);
        setIsProvisioning(true);
        setProvisioningStatus(status);
        return;
      }
      // Pipeline ready — safe to load models
      return client.getModels(tenantSlug).then(models => {
        setAvailableModels(models);
        setModelsLoading(false);
      });
    }).catch(err => {
      const msg = (err as Error).message || "";
      setModelsLoading(false);
      if (msg.includes("No star schema tables") || msg.includes("Run dbt first") || msg.includes("bsl_column_catalog")) {
        setIsProvisioning(true);
      } else {
        setModelsError(msg || "Failed to connect to analytics backend");
      }
    });
  }, [tenantSlug, availableModels.length, modelsLoading, modelsError, isProvisioning]);

  // Check backend LLM health on mount and periodically
  useEffect(() => {
    if (!tenantSlug) return;

    const client = createPlatformAPIClient();
    const checkLLM = async () => {
      const status = await client.checkBackendLLM();
      setBackendLLMAvailable(status.available);
      setBackendLLMProvider(status.provider);
    };

    checkLLM();
    // Re-check every 30s in case Ollama starts/stops
    const interval = setInterval(checkLLM, 30_000);
    return () => clearInterval(interval);
  }, [tenantSlug]);

  // Initialize WebLLM in background once models are loaded
  useEffect(() => {
    if (webllmReady || webllmLoading || availableModels.length === 0) return;

    setWebllmLoading(true);
    setWebllmStatus("Loading AI model...");

    (async () => {
      try {
        const { WebLLMSemanticHandler } = await import(
          "../dashboard/smarter_dashboard/../../../utils/smarter/autovisualization_dashboard/webllm-handler.ts"
        );
        const llmHandler = new WebLLMSemanticHandler({}, "3b");
        await llmHandler.initialize((p: { progress: number; text: string }) => {
          setWebllmProgress(p.progress || 0);
          setWebllmStatus(p.text || "Loading AI model...");
        });
        setWebllmEngine(llmHandler);
        setWebllmReady(true);
        setWebllmStatus("Ready");
      } catch (err) {
        console.error("WebLLM init failed:", err);
        setWebllmStatus(`Error: ${(err as Error).message}`);
      } finally {
        setWebllmLoading(false);
      }
    })();
  }, [availableModels.length, webllmReady, webllmLoading]);

  // No tenant configured
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

  // Provisioning animation
  if (isProvisioning) {
    const statusLabel = provisioningStatus?.status || "starting";
    const statusMessages: Record<string, { label: string; progress: number }> = {
      starting: { label: "Generating mock data...", progress: 10 },
      ingesting: { label: "Ingesting raw data...", progress: 25 },
      modeling: { label: "Building star schema...", progress: 50 },
      cataloging: { label: "Indexing semantic layer...", progress: 75 },
      error: { label: "Pipeline error -- retrying...", progress: 0 },
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

  // Backend error
  if (modelsError) {
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
            <p class="text-gata-cream/40 text-sm leading-relaxed">{modelsError}</p>
          </div>
          <div class="flex flex-col gap-4">
            <button
              type="button"
              onClick={() => { setModelsError(null); setModelsLoading(false); }}
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

  // Loading models
  if (modelsLoading || availableModels.length === 0) {
    return (
      <div class="min-h-screen flex items-center justify-center bg-gata-dark">
        <div class="text-center space-y-6">
          <div class="w-16 h-16 border-4 border-gata-green/30 border-t-gata-green rounded-full animate-spin mx-auto" />
          <p class="text-gata-green font-mono text-sm uppercase tracking-widest">Loading Star Schema Models...</p>
        </div>
      </div>
    );
  }

  // Dashboard workspace — shown immediately once models load.
  // WebLLM loads in background; chat is disabled until both LLMs are ready.
  return (
    <div class="relative">
      {/* WebLLM loading indicator (floating, non-blocking) */}
      {webllmLoading && (
        <div class="fixed bottom-4 right-4 z-50 bg-gata-dark/90 backdrop-blur-sm border border-gata-green/20 rounded-xl px-4 py-3 shadow-xl max-w-xs">
          <div class="flex items-center gap-3">
            <div class="w-5 h-5 border-2 border-gata-green/30 border-t-gata-green rounded-full animate-spin flex-shrink-0" />
            <div class="min-w-0">
              <p class="text-[10px] font-black text-gata-green uppercase tracking-widest">Loading WebLLM</p>
              <p class="text-[9px] text-gata-cream/40 truncate">{webllmStatus}</p>
            </div>
            <span class="text-[10px] text-gata-cream/30 font-bold flex-shrink-0">{Math.round(webllmProgress * 100)}%</span>
          </div>
          <div class="mt-2 w-full bg-gata-dark/60 rounded-full h-1 overflow-hidden">
            <div
              class="h-1 bg-gata-green rounded-full transition-all duration-500"
              style={{ width: `${webllmProgress * 100}%` }}
            />
          </div>
        </div>
      )}

      <CustomDataDashboard
        webllmEngine={webllmEngine}
        webllmReady={webllmReady}
        backendLLMAvailable={backendLLMAvailable}
        backendLLMProvider={backendLLMProvider}
        tenantSlug={tenantSlug}
        models={availableModels}
        onShowObservability={() => setShowObservability(true)}
      />
    </div>
  );
}
