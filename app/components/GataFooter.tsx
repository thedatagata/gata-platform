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
                    <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 01-2.063-2.065 2.064 2.064 0 112.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/></svg>
                 </a>
                 <a href="https://github.com/thedatagata" target="_blank" class="w-12 h-12 rounded-full border border-gata-green/10 flex items-center justify-center hover:bg-gata-green hover:text-gata-dark transition-all">
                    <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12"/></svg>
                 </a>
                 <a href="mailto:thedatagata@gmail.com" class="w-12 h-12 rounded-full border border-gata-green/10 flex items-center justify-center hover:bg-gata-green hover:text-gata-dark transition-all">
                    <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M1.5 8.67v8.58a3 3 0 003 3h15a3 3 0 003-3V8.67l-8.928 5.493a3 3 0 01-3.144 0L1.5 8.67z"/><path d="M22.5 6.908V6.75a3 3 0 00-3-3h-15a3 3 0 00-3 3v.158l9.714 5.978a1.5 1.5 0 001.572 0L22.5 6.908z"/></svg>
                 </a>
            </div>
          </div>

          <div class="space-y-6">
            <h4 class="text-[10px] font-black text-gata-green uppercase tracking-[0.4em] mb-8">Navigation</h4>
            <nav class="flex flex-col gap-4">
               <a href="#experience" class="text-sm font-bold uppercase tracking-widest text-gata-cream/60 hover:text-gata-green transition-colors">Experience Loop</a>
               <a href="/demo" class="text-sm font-bold uppercase tracking-widest text-gata-cream/60 hover:text-gata-green transition-colors">Product Demo</a>
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
