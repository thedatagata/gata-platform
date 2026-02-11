
interface HeroFeatureProps {
  _ldClientId?: string;
}

export default function HeroFeature({ _ldClientId }: HeroFeatureProps) {
  return (
    <section class="relative min-h-screen bg-gata-dark overflow-hidden">
      {/* Background with Swamp Image */}
      <div class="absolute inset-0 z-0">
        <img
          src="/gata_app_utils/nerdy_alligator_swamp.png"
          alt="Data Swamp"
          class="w-full h-full object-cover opacity-60"
        />
        <div class="absolute inset-0 bg-gradient-to-b from-gata-dark/80 via-gata-dark/50 to-gata-dark/90"></div>
      </div>

      <div class="relative z-10 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pt-32 pb-20">
        {/* Main Hero Header */}
        <div class="mb-20 animate-fade-in-up">
          <h1 class="text-7xl md:text-9xl font-black text-gata-cream italic tracking-tighter uppercase mb-4 leading-none">
            DATA_<span class="text-gata-green">GATA</span>
          </h1>
          <p class="text-lg md:text-xl text-gata-cream/80 font-medium italic max-w-3xl leading-relaxed">
            "Ah you think having a data swamp is unique? You merely implemented a data 
            swamp in pursuit of a single source of truth. I was born in the swamp, molded by it..."
          </p>
        </div>

        {/* Legend Has It Section */}
        <div id="legend" class="grid md:grid-cols-2 gap-12 items-center mt-20 scroll-mt-32">
          <div class="relative group">
            <div class="absolute -inset-2 bg-gata-green/20 rounded-[2rem] blur-xl opacity-0 group-hover:opacity-100 transition duration-500"></div>
            <div class="relative bg-gata-cream p-4 rounded-[2rem] shadow-2xl transform -rotate-2 hover:rotate-0 transition-transform duration-500 border-4 border-gata-green">
              <img
                src="/gata_app_utils/water_cooler.png"
                alt="Nerdy Alligator at Water Cooler"
                class="w-full h-auto rounded-[1.5rem]"
              />
              <div class="absolute bottom-6 right-6 bg-gata-green text-gata-dark font-black px-4 py-1 rounded-full text-xs uppercase tracking-widest animate-bounce">
                LEGENDARY 
              </div>
            </div>
          </div>

          <div class="space-y-8">
            <div>
              <h2 class="text-4xl md:text-5xl font-black text-gata-green italic uppercase tracking-tighter mb-8">
                Legend has it
              </h2>
              <div class="space-y-6 text-xl text-gata-cream font-bold leading-relaxed">
                <p class="bg-gata-dark/90 backdrop-blur-sm px-6 py-3 rounded-2xl border border-gata-green/20 shadow-xl inline-block">
                  According to Silicon Valley folklore, some early stage start-ups have reported to 
                  have encountered what can only be described as the "DATA_GATA".
                </p>
                <p class="bg-gata-dark/90 backdrop-blur-sm px-6 py-3 rounded-2xl border border-gata-green/20 shadow-xl inline-block">
                  This creature evolved in an environment where a bootstrapped start-up sold 
                  data-products as supported production features to raise capital to build the product 
                  angel investors were sold on.
                </p>
                <p class="bg-gata-dark/90 backdrop-blur-sm px-6 py-3 rounded-2xl border border-gata-green/20 shadow-xl inline-block">
                  In this environment, this once ambitious young college dropout evolved into what 
                  can only be described as a half datar, half gator monstrosity that survives on 
                  leading data teams out of the swamp to the promiseland of a true single source of 
                  truth data platform.
                </p>
              </div>
            </div>

            <div class="flex flex-wrap gap-4 pt-6">
              <a
                href="/auth/signin"
                class="px-8 py-4 bg-gata-green text-gata-dark rounded-full text-sm font-black uppercase tracking-[0.2em] hover:bg-[#a0d147] transition-all transform hover:-translate-y-1 shadow-[0_20px_40px_rgba(144,193,55,0.3)]"
              >
                Launch App Experience
              </a>
              <a
                href="#experience"
                class="px-8 py-4 border-2 border-gata-green text-gata-green bg-gata-dark/60 rounded-full text-sm font-black uppercase tracking-[0.2em] hover:bg-gata-green hover:text-gata-dark transition-all"
              >
                Track Record
              </a>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
