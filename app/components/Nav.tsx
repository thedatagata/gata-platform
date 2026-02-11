export default function Nav() {
  return (
    <nav class="fixed w-full z-50 transition-all duration-300 bg-gata-dark/80 backdrop-blur-xl py-5 border-b border-gata-green/10">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between items-center">
          {/* Logo */}
          <a href="/" class="group flex items-center space-x-3 transition-transform hover:scale-105">
            <div class="w-12 h-12 rounded-2xl overflow-hidden border-2 border-gata-green/30 p-1 bg-gata-dark/40 shadow-lg group-hover:border-gata-green transition-all">
              <img
                src="/gata_app_utils/nerdy_alligator_headshot.png"
                alt="DATA_GATA"
                class="w-full h-full object-cover rounded-xl"
              />
            </div>
            <span class="text-2xl font-black text-gata-cream italic tracking-tighter uppercase">
              DATA_<span class="text-gata-green">GATA</span> 
            </span>
          </a>

          {/* Navigation - Desktop */}
          <div class="hidden md:flex items-center space-x-10">
             <a href="#legend" class="text-[10px] font-black text-gata-cream/60 uppercase tracking-[0.3em] hover:text-gata-green transition-colors">Legend</a>
             <a href="#experience" class="text-[10px] font-black text-gata-cream/60 uppercase tracking-[0.3em] hover:text-gata-green transition-colors">Experience</a>
             <a href="https://linkedin.com/in/yalenewman" target="_blank" class="text-[10px] font-black text-gata-cream/60 uppercase tracking-[0.3em] hover:text-gata-green transition-colors">LinkedIn</a>
             <a
              href="/auth/signin"
              class="px-8 py-3 bg-gata-green text-gata-dark rounded-full text-[10px] font-black uppercase tracking-[0.3em] hover:bg-[#a0d147] transition-all transform hover:-translate-y-0.5 shadow-lg shadow-gata-green/20"
            >
              Launch Demo
            </a>
          </div>

          {/* Mobile Menu Toggle (Simplified) */}
          <div class="md:hidden">
            <a
              href="/auth/signin"
              class="px-5 py-2.5 bg-gata-green text-gata-dark rounded-full text-[10px] font-black uppercase tracking-[0.2em]"
            >
              Demo
            </a>
          </div>
        </div>
      </div>
    </nav>
  );
}