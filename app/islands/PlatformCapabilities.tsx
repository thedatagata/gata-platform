export default function PlatformCapabilities() {
  const capabilities = [
    {
      title: "Multi-Tenant Star Schema",
      description:
        "Isolated analytics per customer with shared infrastructure. Each tenant gets their own fact and dimension tables.",
      metric: "18",
      metricLabel: "Star Schema Tables",
    },
    {
      title: "Natural Language Queries",
      description:
        "Ask questions in plain English. The semantic layer translates intent into optimized SQL across your star schema.",
      metric: "6",
      metricLabel: "Semantic Models",
    },
    {
      title: "13 Source Connectors",
      description:
        "Facebook, Google, Bing, LinkedIn, Amazon ads. Shopify, WooCommerce, BigCommerce. GA4, Amplitude, Mixpanel.",
      metric: "13",
      metricLabel: "Connectors",
    },
  ];

  return (
    <section class="py-32 bg-gata-dark relative overflow-hidden">
      {/* Subtle grid background */}
      <div
        class="absolute inset-0 opacity-5"
        style="background-image: linear-gradient(rgba(144,193,55,.3) 1px, transparent 1px), linear-gradient(90deg, rgba(144,193,55,.3) 1px, transparent 1px); background-size: 60px 60px;"
      />

      <div class="relative z-10 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="text-center mb-20">
          <h4 class="text-[10px] font-black text-gata-green uppercase tracking-[0.5em] mb-4">
            Platform Architecture
          </h4>
          <h2 class="text-5xl md:text-6xl font-black text-gata-cream italic tracking-tighter uppercase">
            What's Under <span class="text-gata-green">the Hood</span>
          </h2>
        </div>

        <div class="grid md:grid-cols-3 gap-8">
          {capabilities.map((cap, i) => (
            <div
              key={i}
              class="group bg-gata-dark/60 border border-gata-green/10 rounded-3xl p-10 hover:border-gata-green/30 transition-all duration-500"
            >
              <div class="mb-8">
                <span class="text-5xl font-black text-gata-green italic">
                  {cap.metric}
                </span>
                <span class="text-[10px] font-black text-gata-cream/30 uppercase tracking-widest ml-2">
                  {cap.metricLabel}
                </span>
              </div>
              <h3 class="text-xl font-black text-gata-cream uppercase tracking-tight mb-4">
                {cap.title}
              </h3>
              <p class="text-sm text-gata-cream/50 leading-relaxed font-medium">
                {cap.description}
              </p>
            </div>
          ))}
        </div>

        <div class="text-center mt-16">
          <a
            href="/demo"
            class="inline-block px-10 py-4 bg-gata-green text-gata-dark rounded-full text-xs font-black uppercase tracking-[0.3em] hover:bg-[#a0d147] transition-all transform hover:-translate-y-1 shadow-lg shadow-gata-green/20"
          >
            See It In Action
          </a>
        </div>
      </div>
    </section>
  );
}
