// islands/onboarding/ExperienceGateway.tsx

interface ExperienceGatewayProps {
  onSelect: (mode: 'sample' | 'custom' | 'connected') => void;
  tenantSlug?: string;
}

export default function ExperienceGateway({ onSelect, tenantSlug }: ExperienceGatewayProps) {
  const showConnected = !!tenantSlug;

  return (
    <div class="min-h-screen bg-gradient-to-br from-gata-dark to-gata-darker flex items-center justify-center p-4">
      <div class="max-w-5xl w-full">
        <div class="text-center mb-16">
          <h4 class="text-[10px] font-black text-gata-green uppercase tracking-[0.4em] mb-4">Discovery Engine Phase 01: ORIGIN</h4>
          <h1 class="text-6xl font-black text-gata-cream italic tracking-tighter uppercase mb-4 leading-none">
             Analyze <span class="text-gata-green">Anything.</span>
          </h1>
          <p class="text-xs text-gata-cream/40 font-medium uppercase tracking-[0.2em] max-w-lg mx-auto leading-relaxed">
            Universal semantic layer for local datasets. Choose your initialization vector.
          </p>
        </div>

        <div class={`grid gap-10 px-4 ${showConnected ? "md:grid-cols-3" : "md:grid-cols-2"}`}>
          {/* Connected Analytics Card — primary for tenanted users */}
          {showConnected && (
            <button
              type="button"
              onClick={() => onSelect('connected')}
              class="group text-left relative bg-gata-dark/40 backdrop-blur-xl rounded-[3rem] p-10 border-2 border-gata-green/30 hover:border-gata-green transition-all duration-500 hover:shadow-[0_0_60px_rgba(144,193,55,0.15)] transform hover:-translate-y-2 overflow-hidden"
            >
              <div class="relative z-10 flex flex-col h-full">
                <div class="w-16 h-16 bg-gata-green/15 rounded-2xl flex items-center justify-center mb-8 group-hover:scale-110 transition-transform">
                  <svg class="w-8 h-8 text-gata-green" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
                  </svg>
                </div>
                <h3 class="text-3xl font-black text-gata-cream italic uppercase tracking-tighter mb-4 pr-12">Connected Analytics</h3>
                <p class="text-sm text-gata-cream/60 mb-8 flex-grow leading-relaxed font-medium">
                  Explore your star schema — ad performance, orders, sessions, events, campaigns, and users.
                </p>
                <div class="flex items-center text-xs font-black text-gata-green uppercase tracking-[0.2em] group-hover:translate-x-2 transition-transform">
                  Open Star Schema →
                </div>
              </div>
              {/* Prominent glow highlight */}
              <div class="absolute inset-0 bg-gradient-to-br from-gata-green/8 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
            </button>
          )}

          {/* Sample Experience Card */}
          <button
            type="button"
            onClick={() => onSelect('sample')}
            class="group text-left relative bg-gata-dark/40 backdrop-blur-xl rounded-[3rem] p-10 border border-gata-green/10 hover:border-gata-green/40 transition-all duration-500 hover:shadow-[0_0_50px_rgba(144,193,55,0.1)] transform hover:-translate-y-2 overflow-hidden"
          >
            <div class="relative z-10 flex flex-col h-full">
              <div class="w-16 h-16 bg-gata-green/10 rounded-2xl flex items-center justify-center mb-8 group-hover:scale-110 transition-transform">
                <span class="text-3xl grayscale group-hover:grayscale-0 transition-all"></span>
              </div>
              <h3 class="text-3xl font-black text-gata-cream italic uppercase tracking-tighter mb-4 pr-12">Explore Sample Dataset</h3>
              <p class="text-sm text-gata-cream/60 mb-8 flex-grow leading-relaxed font-medium">
                Jump into a pre-configured B2B SaaS dataset with 345K rows. See how the AI identifies trends, builds charts, and populates cubes instantly.
              </p>
              <div class="flex items-center text-xs font-black text-gata-green uppercase tracking-[0.2em] group-hover:translate-x-2 transition-transform">
                Launch Environment →
              </div>
            </div>
            {/* Glossy top highlight */}
            <div class="absolute inset-0 bg-gradient-to-br from-gata-green/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
          </button>

          {/* Custom Experience Card */}
          <button
            type="button"
            onClick={() => onSelect('custom')}
            class="group text-left relative bg-gata-dark/40 backdrop-blur-xl rounded-[3rem] p-10 border border-gata-green/10 hover:border-gata-green/40 transition-all duration-500 hover:shadow-[0_0_50px_rgba(144,193,55,0.1)] transform hover:-translate-y-2 overflow-hidden"
          >
            <div class="relative z-10 flex flex-col h-full">
              <div class="w-16 h-16 bg-gata-green/10 rounded-2xl flex items-center justify-center mb-8 group-hover:scale-110 transition-transform">
                <span class="text-3xl grayscale group-hover:grayscale-0 transition-all"></span>
              </div>
              <h3 class="text-3xl font-black text-gata-cream italic uppercase tracking-tighter mb-4 pr-12">Analyze Your Own Data</h3>
              <p class="text-sm text-gata-cream/60 mb-8 flex-grow leading-relaxed font-medium">
                Upload CSV, Parquet, or JSON. Our automated profiler builds a semantic layer so you can query local storage in plain English.
              </p>
              <div class="flex items-center text-xs font-black text-gata-green uppercase tracking-[0.2em] group-hover:translate-x-2 transition-transform">
                Initialize Custom Stream →
              </div>
            </div>
            {/* Glossy top highlight */}
            <div class="absolute inset-0 bg-gradient-to-br from-gata-green/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
          </button>
        </div>
      </div>
    </div>
  );
}
