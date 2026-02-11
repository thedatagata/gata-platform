// islands/dashboard/smarter_dashboard/SmartDashLoadingPage.tsx
import { useEffect, useState } from "preact/hooks";
import { TableProfiler } from "../../../utils/services/table-profiler.ts";
import { metadataStore } from "../../../utils/services/metadata-store.ts";
import { createSemanticTables } from "../../../utils/smarter/dashboard_utils/semantic-objects.ts";
import { WebLLMSemanticHandler } from "../../../utils/smarter/autovisualization_dashboard/webllm-handler.ts";
import { getLDClient } from "../../../utils/launchdarkly/client.ts";

export interface LoadingProgress {
  step: "duckdb" | "semantic" | "webllm" | "complete";
  progress: number;
  message: string;
}

interface LoadingPageProps {
  onComplete: (db: any, webllmEngine: any) => void;
  motherDuckToken: string;
  mode: 'sample' | 'custom';
}

export default function SmartDashLoadingPage({ onComplete, motherDuckToken, mode }: LoadingPageProps) {
  const [loading, setLoading] = useState<LoadingProgress>({
    step: "duckdb",
    progress: 0,
    message: "Initializing database...",
  });

  useEffect(() => {
    async function initialize() {
      try {
        // Step 1: Initialize DuckDB WASM (Needed for both modes)
        setLoading({
          step: "duckdb",
          progress: 10,
          message: "Loading DuckDB WASM...",
        });

        // Use standard duckdb-wasm if motherduck isn't needed, or motherduck otherwise
        let duckdb: any;
        let localConn: any;

        const { MDConnection, getAsyncDuckDb } = await import("@motherduck/wasm-client");

        if (mode === 'sample') {
          // Step 0: Check Usage Limit (Only for Sample mode as it hits cloud resources)
          setLoading({
            step: "duckdb",
            progress: 5,
            message: "Verifying access...",
          });

          try {
            const usageRes = await fetch('/api/demo/usage', { method: 'POST' });
            const usageData = await usageRes.json();
            
            if (!usageRes.ok) {
               if (usageRes.status === 403) {
                 throw new Error(`Demo limit reached (${usageData.usage}/${usageData.limit} loads). Contact admin.`);
               }
               // If 401, it means not a demo user, so we proceed (unlimited)
               if (usageRes.status !== 401) {
                  console.warn("Usage check warning:", usageData);
               }
            }
          } catch (err) {
            // If it's the limit error, rethrow
            if (err instanceof Error && err.message.includes("Demo limit")) {
              throw err;
            }
            // Otherwise log and proceed (fail open for network errors to avoid blocking valid users)
            console.error("Usage check failed, proceeding:", err);
          }
          
          setLoading({
            step: "duckdb",
            progress: 20,
            message: "Connecting to MotherDuck...",
          });

          const mdConn = await MDConnection.create({ mdToken: motherDuckToken });
          await mdConn.isInitialized();

          setLoading({
            step: "duckdb",
            progress: 30,
            message: "Materializing sample data...",
          });

          const sessionsResult = await mdConn.evaluateStreamingQuery('SELECT * FROM my_db.amplitude.sessions_pivot_src');
          const sessionsBatches = await sessionsResult.arrowStream.readAll();
          const Arrow = await import('apache-arrow');
          const sessionsArrow = new Arrow.Table(sessionsBatches);

          // Fix: getAsyncDuckDb expects MDConnectionParams
          duckdb = await getAsyncDuckDb({ mdToken: motherDuckToken });
          localConn = await duckdb.connect();

          // Set search path early to ensure subsequent operations use the right context
          try {
            await localConn.query("SET search_path = 'memory.main,temp.main,main,my_db.amplitude,my_db.main'");
            console.log(" [SmartDash] Search path configured for MotherDuck/Local hybrid");
          } catch (e) {
            console.warn(" [SmartDash] Failed to set search path early:", e);
          }

          await localConn.insertArrowTable(sessionsArrow, { name: 'session_facts' });

          setLoading({
            step: "duckdb",
            progress: 40,
            message: "Materializing user data...",
          });

          const usersResult = await mdConn.evaluateStreamingQuery('SELECT * FROM my_db.amplitude.users_pivot_src');
          const usersBatches = await usersResult.arrowStream.readAll();
          const usersArrow = new Arrow.Table(usersBatches);
          await localConn.insertArrowTable(usersArrow, { name: 'users_dim' });

          setLoading({
            step: "duckdb",
            progress: 48,
            message: "Profiling sample tables...",
          });
          
          if (await TableProfiler.needsProfiling("session_facts")) {
            const sessionsMetadata = await TableProfiler.profileTable(localConn, "session_facts");
            await metadataStore.saveTableMetadata("session_facts", sessionsMetadata);
          }
          if (await TableProfiler.needsProfiling("users_dim")) {
            const usersMetadata = await TableProfiler.profileTable(localConn, "users_dim");
            await metadataStore.saveTableMetadata("users_dim", usersMetadata);
          }
        } else {
          // CUSTOM MODE: Just plain DuckDB
          console.log(" [SmartDash] Initializing in CUSTOM mode (Local Files Only)");
          
          setLoading({
            step: "duckdb",
            progress: 20,
            message: "Initializing local engine...",
          });

          // Even in custom mode, we initialize the MotherDuck WASM client to ensure assets are correctly loaded.
          // We use a dummy token if none provided to satisfy internal validation.
          const effectiveToken = motherDuckToken || "local";
          const mdConn = await MDConnection.create({ mdToken: effectiveToken });
          await mdConn.isInitialized();
          
          duckdb = await getAsyncDuckDb({ mdToken: effectiveToken });
          localConn = await duckdb.connect();
          
          try {
            await localConn.query("SET search_path = 'memory.main,temp.main,main'");
          } catch (e) {
            console.warn(" [SmartDash] Failed to set search path for custom mode:", e);
          }
          
          setLoading({
            step: "duckdb",
            progress: 40,
            message: "Local engine ready...",
          });

          // Small delay for drama/smoothness
          await new Promise(r => setTimeout(r, 800));
        }

        // Step 2: Initialize semantic layer
        setLoading({
          step: "semantic",
          progress: 50,
          message: "Loading semantic layer...",
        });

        // In custom mode, we might not have any semantic tables yet
        const semanticTables = createSemanticTables(localConn);

        setLoading({
          step: "semantic",
          progress: 55,
          message: "Semantic layer ready...",
        });

        // Step 3: Initialize WebLLM
        setLoading({
          step: "webllm",
          progress: 60,
          message: "Loading AI assistant...",
        });

        // Initialize WebLLM with semantic layer and model tier from LaunchDarkly
        const ldClient = getLDClient();
        
        // Get model tier from LaunchDarkly flag
        const modelTier = (ldClient as any)?.variation("smarter-model-tier", "3b") || "3b";
        console.log(` [SmartDash] Model tier: ${modelTier}`);
        
        const llmHandler = new WebLLMSemanticHandler(semanticTables, modelTier, ldClient);
        await llmHandler.initialize((progress) => {
          const webllmProgress = 60 + (progress.progress || 0) * 35;
          setLoading({
            step: "webllm",
            progress: webllmProgress,
            message: progress.text || "Loading AI model...",
          });
        });

        setLoading({
          step: "complete",
          progress: 100,
          message: "Setup complete! Launching dashboard...",
        });

        await new Promise((resolve) => setTimeout(resolve, 500));

        // Attach the db instance to the connection so utility functions can find it (e.g. for registerFileHandle)
        (localConn as any).db = duckdb;

        onComplete(localConn, llmHandler);
      } catch (error) {
        console.error("Initialization failed:", error);
        setLoading({
          step: "duckdb",
          progress: 0,
          message: `Error: ${(error as Error).message}`,
        });
      }
    }

    initialize();
  }, [motherDuckToken, mode]);

  return (
    <div class="min-h-screen bg-gradient-to-br from-gata-dark to-[#186018] flex items-center justify-center p-4">
      <div class="max-w-md w-full">
        <div class="text-center mb-8">
          <h1 class="text-4xl font-bold text-gata-cream mb-2">
            DATA_<span class="text-gata-green">GATA</span> Analytics
          </h1>
          <p class="text-gata-cream/80">Setting up your smart dashboard</p>
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
              {loading.step === "duckdb" && (mode === 'sample' ? "Downloading data from cloud..." : "Initializing WASM engine...")}
              {loading.step === "semantic" && "Preparing smart queries..."}
              {loading.step === "webllm" && "Loading AI assistant..."}
              {loading.step === "complete" && "Ready to go!"}
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

          <div class="flex justify-between text-xs mt-6">
            <div class={`flex items-center ${loading.progress >= 10 ? "text-gata-green" : "text-gata-cream/40"}`}>
              <div class={`w-2 h-2 rounded-full mr-2 ${loading.progress >= 50 ? "bg-gata-green" : loading.progress >= 10 ? "bg-gata-green/60" : "bg-gata-cream/20"}`} />
              <span class="font-medium">Database</span>
            </div>
            <div class={`flex items-center ${loading.progress >= 50 ? "text-gata-green" : "text-gata-cream/40"}`}>
              <div class={`w-2 h-2 rounded-full mr-2 ${loading.progress >= 60 ? "bg-gata-green" : loading.progress >= 50 ? "bg-gata-green/60" : "bg-gata-cream/20"}`} />
              <span class="font-medium">Semantic</span>
            </div>
            <div class={`flex items-center ${loading.progress >= 60 ? "text-gata-green" : "text-gata-cream/40"}`}>
              <div class={`w-2 h-2 rounded-full mr-2 ${loading.progress >= 95 ? "bg-gata-green" : loading.progress >= 60 ? "bg-gata-green/60" : "bg-gata-cream/20"}`} />
              <span class="font-medium">AI</span>
            </div>
          </div>
        </div>

        <div class="text-center mt-6 text-sm text-gata-cream/70">
          <p> All queries run locally in your browser</p>
        </div>
      </div>
    </div>
  );
}