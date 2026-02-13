import { useEffect, useState } from "preact/hooks";
import { WebLLMSemanticHandler } from "../../../utils/smarter/autovisualization_dashboard/webllm-handler.ts";

export interface LoadingProgress {
  step: "webllm" | "complete";
  progress: number;
  message: string;
}

interface LoadingPageProps {
  // deno-lint-ignore no-explicit-any
  onComplete: (webllmEngine: any) => void;
}

export default function SmartDashLoadingPage({ onComplete }: LoadingPageProps) {
  const [loading, setLoading] = useState<LoadingProgress>({
    step: "webllm",
    progress: 0,
    message: "Initializing AI assistant...",
  });

  useEffect(() => {
    async function initializeWebLLM() {
      try {
        setLoading({
          step: "webllm",
          progress: 5,
          message: "Loading Qwen 2.5 Coder 3B...",
        });

        const llmHandler = new WebLLMSemanticHandler({}, "3b");
        await llmHandler.initialize((p) => {
          setLoading({
            step: "webllm",
            progress: 5 + (p.progress || 0) * 90,
            message: p.text || "Loading AI model...",
          });
        });

        setLoading({ step: "complete", progress: 100, message: "Ready!" });
        await new Promise(r => setTimeout(r, 400));
        onComplete(llmHandler);
      } catch (err) {
        console.error("WebLLM init failed", err);
        setLoading({ step: "webllm", progress: 0, message: `Error: ${(err as Error).message}` });
      }
    }

    initializeWebLLM();
  }, []);

  return (
    <div class="min-h-screen bg-gradient-to-br from-gata-dark to-[#186018] flex items-center justify-center p-4">
      <div class="max-w-md w-full">
        <div class="text-center mb-8">
          <h1 class="text-4xl font-bold text-gata-cream mb-2">
            DATA_<span class="text-gata-green">GATA</span> Analytics
          </h1>
          <p class="text-gata-cream/80">Preparing your AI assistant</p>
        </div>

        <div class="bg-gata-dark/80 backdrop-blur-sm rounded-2xl shadow-xl p-8 border border-gata-green/20">
          <div class="flex justify-center mb-6">
            <div class="relative">
              <div class="w-20 h-20 border-4 border-gata-green/30 rounded-full"></div>
              {loading.step === "complete" ? (
                <div class="w-20 h-20 border-4 border-gata-green rounded-full absolute top-0 flex items-center justify-center">
                  <svg class="w-10 h-10 text-gata-green" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                  </svg>
                </div>
              ) : (
                <div class="w-20 h-20 border-4 border-gata-green rounded-full border-t-transparent animate-spin absolute top-0"></div>
              )}
            </div>
          </div>

          <div class="text-center mb-4">
            <p class="text-lg font-medium text-gata-cream mb-1">
              {loading.message}
            </p>
            <p class="text-sm text-gata-cream/70">
              {loading.step === "webllm" && "Loading in-browser AI model for query generation..."}
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
