export default function GataFooter() {
  const year = new Date().getFullYear();

  return (
    <footer class="bg-gata-dark text-gata-cream py-24 relative overflow-hidden">
      {/* Abstract Background */}
      <div class="absolute bottom-0 right-0 w-[40%] h-[40%] bg-gata-green/5 blur-[120px]" />
      
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 relative z-10">
        <div class="grid md:grid-cols-2 lg:grid-cols-4 gap-16 mb-20">
          <div class="space-y-8 col-span-1 md:col-span-2">
            <div class="flex items-center gap-3">
              <div class="w-12 h-12 rounded-2xl bg-gata-green/10 flex items-center justify-center border border-gata-green/20">
                <img src="/gata_app_utils/nerdy_alligator_headshot.png" alt="DATA_GATA" class="h-8 w-8 rounded-lg" />
              </div>
              <h3 class="text-3xl font-black italic tracking-tighter uppercase">
                DATA_<span class="text-gata-green">GATA</span> 
              </h3>
            </div>
            <p class="text-xl text-gata-cream/40 max-w-sm leading-relaxed font-medium">
              Architecting production-ready data ecosystems and semantic layers for the modern enterprise.
            </p>
            <div class="flex gap-6">
                 <a href="https://linkedin.com/in/yalenewman" target="_blank" class="w-12 h-12 rounded-full border border-gata-green/10 flex items-center justify-center hover:bg-gata-green hover:text-gata-dark transition-all">
                    <i class="fab fa-linkedin-in text-lg"></i>
                 </a>
                 <a href="https://github.com/thedatagata" target="_blank" class="w-12 h-12 rounded-full border border-gata-green/10 flex items-center justify-center hover:bg-gata-green hover:text-gata-dark transition-all">
                    <i class="fab fa-github text-lg"></i>
                 </a>
                 <a href="mailto:thedatagata@gmail.com" class="w-12 h-12 rounded-full border border-gata-green/10 flex items-center justify-center hover:bg-gata-green hover:text-gata-dark transition-all">
                    <i class="fas fa-envelope text-lg"></i>
                 </a>
            </div>
          </div>

          <div class="space-y-6">
            <h4 class="text-[10px] font-black text-gata-green uppercase tracking-[0.4em] mb-8">Navigation</h4>
            <nav class="flex flex-col gap-4">
               <a href="#experience" class="text-sm font-bold uppercase tracking-widest text-gata-cream/60 hover:text-gata-green transition-colors">Experience Loop</a>
               <a href="/auth/signin" class="text-sm font-bold uppercase tracking-widest text-gata-cream/60 hover:text-gata-green transition-colors">Product Demo</a>
               <a href="https://github.com/thedatagata" class="text-sm font-bold uppercase tracking-widest text-gata-cream/60 hover:text-gata-green transition-colors">Research Repo</a>
            </nav>
          </div>

          <div class="space-y-6">
            <h4 class="text-[10px] font-black text-gata-green uppercase tracking-[0.4em] mb-8">Contact</h4>
            <div class="space-y-4">
                <p class="text-sm font-bold uppercase tracking-widest text-gata-cream/60 leading-none">Raleigh, NC</p>
                <p class="text-sm font-bold uppercase tracking-widest text-gata-cream/60 leading-none">919-491-6557</p>
                <a href="mailto:thedatagata@gmail.com" class="text-sm font-bold uppercase tracking-widest text-gata-green italic hover:underline">thedatagata@gmail.com</a>
            </div>
          </div>
        </div>

        <div class="pt-12 border-t border-gata-green/10 flex flex-col md:flex-row justify-between items-center gap-6">
          <p class="text-[10px] font-black text-gata-cream/20 uppercase tracking-[0.3em]">
            © {year} DATA_GATA LLC. Built with Fresh & DuckDB.
          </p>
          <div class="flex gap-8">
            <a href="/privacy" class="text-[8px] font-black text-gata-cream/20 hover:text-gata-green uppercase tracking-widest transition-colors">Privacy Policy</a>
            <span class="text-[8px] font-black text-gata-cream/10 uppercase tracking-widest">Terms of Service</span>
          </div>
        </div>
      </div>
    </footer>
  );
}
