import { useEffect, useState } from "preact/hooks";
import { createSemanticTables } from "../../../utils/smarter/dashboard_utils/semantic-objects.ts";
import { WebLLMSemanticHandler } from "../../../utils/smarter/autovisualization_dashboard/webllm-handler.ts";
import { createPlatformAPIClient } from "../../../utils/api/platform-api-client.ts";

export interface LoadingProgress {
  step: "readiness" | "duckdb" | "semantic" | "webllm" | "complete";
  progress: number;
  message: string;
}

interface LoadingPageProps {
  // deno-lint-ignore no-explicit-any
  onComplete: (db: any, webllmEngine: any) => void;
  motherDuckToken: string;
  mode: "connected";
  tenantSlug?: string;
}

export default function SmartDashLoadingPage({ onComplete, motherDuckToken, tenantSlug }: LoadingPageProps) {
  const [loading, setLoading] = useState<LoadingProgress>({
    step: "readiness",
    progress: 0,
    message: "Initializing...",
  });

  const [isReadyForInit, setIsReadyForInit] = useState(false);

  // 1. Polling for Readiness
  useEffect(() => {
    if (!tenantSlug) return;

    let pollTimeout: number;
    const client = createPlatformAPIClient();
    let delay = 2000;

    const poll = async () => {
      try {
        const status = await client.checkReadiness(tenantSlug);

        if (status.is_ready) {
           setIsReadyForInit(true);
           return; // stop polling
        } else {
           let message = "Waiting for data pipeline...";
           let progress = 5;

           if (status.status === 'ingesting') { message = "Mining the swamp (Ingesting)..."; progress = 15; }
           else if (status.status === 'modeling') { message = "Polishing the star schema..."; progress = 35; }
           else if (status.status === 'cataloging') { message = "Indexing semantic layer..."; progress = 45; }

           setLoading({
             step: "readiness",
             progress,
             message: `${message} (${status.last_load_id || 'init'})`
           });
        }
      } catch (e) {
        console.error("Polling error", e);
        setLoading(prev => ({ ...prev, message: "Connection error..." }));
      }
      delay = Math.min(delay * 1.5, 15000);
      schedulePoll();
    };

    const schedulePoll = () => {
      pollTimeout = setTimeout(poll, delay) as unknown as number;
    };

    poll();
    schedulePoll();

    return () => clearTimeout(pollTimeout);
  }, [tenantSlug]);


  // 2. Main Initialization Sequence
  useEffect(() => {
    if (!isReadyForInit) return;

    async function initializeDuckDB() {
      try {
        setLoading({
          step: "duckdb",
          progress: 50,
          message: "Loading DuckDB WASM...",
        });

        const { MDConnection, getAsyncDuckDb } = await import("@motherduck/wasm-client");

        setLoading({ step: "duckdb", progress: 20, message: "Initializing local engine..." });

        const effectiveToken = motherDuckToken || "local";
        const mdConn = await MDConnection.create({ mdToken: effectiveToken });
        await mdConn.isInitialized();

        const duckdb = await getAsyncDuckDb({ mdToken: effectiveToken });
        const localConn = await duckdb.connect();

        try { await localConn.query("SET search_path = 'memory.main,temp.main,main'"); } catch { /* ignore */ }

        setLoading({ step: "duckdb", progress: 40, message: "Local engine ready..." });

        // Semantic Layer
        setLoading({ step: "semantic", progress: 60, message: "Loading semantic layer..." });
        const semanticTables = createSemanticTables(localConn);

        // WebLLM
        setLoading({ step: "webllm", progress: 70, message: "Loading AI assistant..." });

        const llmHandler = new WebLLMSemanticHandler(semanticTables, "3b");
        await llmHandler.initialize((p) => {
           setLoading({
             step: "webllm",
             progress: 70 + (p.progress || 0) * 30,
             message: p.text || "Loading AI model..."
           });
        });

        // Complete
        setLoading({ step: "complete", progress: 100, message: "Ready!" });
        await new Promise(r => setTimeout(r, 500));
        // deno-lint-ignore no-explicit-any
        (localConn as any).db = duckdb;
        onComplete(localConn, llmHandler);

      } catch (err) {
        console.error("Init failed", err);
        setLoading({ step: "duckdb", progress: 0, message: `Error: ${(err as Error).message}` });
      }
    }

    initializeDuckDB();
  }, [isReadyForInit, motherDuckToken]);

  return (
    <div class="min-h-screen bg-gradient-to-br from-gata-dark to-[#186018] flex items-center justify-center p-4">
      <div class="max-w-md w-full">
        <div class="text-center mb-8">
          <h1 class="text-4xl font-bold text-gata-cream mb-2">
            DATA_<span class="text-gata-green">GATA</span> Analytics
          </h1>
          <p class="text-gata-cream/80">Optimizing Tenant Pipeline</p>
        </div>

        <div class="bg-gata-dark/80 backdrop-blur-sm rounded-2xl shadow-xl p-8 border border-gata-green/20">
          <div class="flex justify-center mb-6">
            <div class="relative">
              <div class="w-20 h-20 border-4 border-gata-green/30 rounded-full"></div>
              <div class="w-20 h-20 border-4 border-gata-green rounded-full border-t-transparent animate-spin absolute top-0"></div>
              {loading.step === "complete" && (
                <div class="absolute inset-0 flex items-center justify-center">
                  <svg class="w-10 h-10 text-gata-green" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                  </svg>
                </div>
              )}
            </div>
          </div>

          <div class="text-center mb-4">
            <p class="text-lg font-medium text-gata-cream mb-1">
              {loading.message}
            </p>
            <p class="text-sm text-gata-cream/70">
              {loading.step === "readiness" && "Waiting for cloud resources..."}
              {loading.step === "duckdb" && "Initializing WASM..."}
              {loading.step === "semantic" && "Preparing smart queries..."}
              {loading.step === "webllm" && "Loading AI assistant..."}
            </p>
          </div>

          <div class="mb-4">
            <div class="flex justify-between text-xs text-gata-cream/70 mb-2">
              <span>Progress</span>
              <span>{Math.round(loading.progress)}%</span>
            </div>
            <div class="w-full bg-gata-dark/60 rounded-full h-3 overflow-hidden border border-gata-green/30">
              <div
                class="h-3 bg-gradient-to-r from-gata-green to-[#a0d147] rounded-full transition-all duration-500 ease-out"
                style={{ width: `${loading.progress}%` }}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
