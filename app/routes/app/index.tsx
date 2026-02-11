import { Head } from "$fresh/runtime.ts";
import Nav from "../../components/Nav.tsx";
import GataFooter from "../../components/GataFooter.tsx";

export default function Home() {
  return (
    <div class="bg-gata-dark min-h-screen">
      <Head>
        <title>DATA_GATA | Analytics Dashboard</title>
      </Head>
      
      <Nav />
      <main class="min-h-screen pt-32 pb-20">
        <div class="max-w-4xl mx-auto px-4">
          <div class="text-center mb-16">
            <h1 class="text-6xl font-black text-gata-cream italic tracking-tighter uppercase mb-4 leading-none">
              Analytics <span class="text-gata-green">Dashboard.</span>
            </h1>
            <p class="text-xs text-gata-cream/40 font-medium uppercase tracking-[0.2em] max-w-lg mx-auto leading-relaxed">
              Natural language queries powered by MotherDuck
            </p>
          </div>

          <div class="bg-gata-dark/40 backdrop-blur-xl border border-gata-green/10 rounded-[3rem] p-12 shadow-2xl max-w-md mx-auto relative overflow-hidden group">
            <div class="absolute inset-0 bg-gradient-to-br from-gata-green/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
            
            <h2 class="text-3xl font-black text-gata-cream italic uppercase tracking-tighter mb-8 pr-12">Get Started</h2>
            <ul class="space-y-4 mb-10 text-gata-cream/60 font-medium">
              <li class="flex items-start">
                <span class="w-2 h-2 rounded-full bg-gata-green mr-4 mt-2 shadow-[0_0_10px_rgba(144,193,55,0.5)]"></span>
                Stream data from MotherDuck
              </li>
              <li class="flex items-start">
                <span class="w-2 h-2 rounded-full bg-gata-green mr-4 mt-2 shadow-[0_0_10px_rgba(144,193,55,0.5)]"></span>
                MotherDuck AI natural language queries
              </li>
              <li class="flex items-start">
                <span class="w-2 h-2 rounded-full bg-gata-green mr-4 mt-2 shadow-[0_0_10px_rgba(144,193,55,0.5)]"></span>
                Interactive visualizations
              </li>
            </ul>
            <a 
              href="/app/dashboard"
              class="block w-full text-center py-4 bg-gata-green text-gata-dark font-black uppercase tracking-[0.2em] text-xs rounded-full hover:bg-[#a0d147] transition-all transform hover:-translate-y-1 shadow-lg shadow-gata-green/20"
            >
              Launch Dashboard 
            </a>
          </div>
        </div>
      </main>
      <GataFooter />
    </div>
  );
}
