import Nav from "../components/Nav.tsx";
import GataFooter from "../components/GataFooter.tsx";

export default function PrivacyPolicy() {
  return (
    <div class="relative min-h-screen bg-gata-dark text-gata-cream">
      <Nav />
      
      <main class="pt-32 pb-20 px-4">
        <div class="max-w-4xl mx-auto space-y-16">
          {/* Header */}
          <header class="text-center space-y-4">
            <h1 class="text-6xl font-black italic tracking-tighter uppercase underline decoration-gata-green decoration-8 underline-offset-8">
              Privacy <span class="text-gata-green">Policy</span>
            </h1>
            <p class="text-gata-cream/40 font-mono text-sm tracking-widest uppercase">Last Updated: January 2026</p>
          </header>

          {/* Intro Card */}
          <section class="bg-gata-dark/40 border border-gata-green/10 p-10 rounded-[3rem] backdrop-blur-xl shadow-2xl">
            <p class="text-xl leading-relaxed italic text-gata-cream/80">
              DATA_GATA respects your privacy. We're a "Small Data" company focused on local-first analysis. This policy explains how we handle your information when you use our platform and Google OAuth services.
            </p>
          </section>

          {/* Content Sections */}
          <div class="space-y-12 px-4">
            <section class="space-y-4">
              <h2 class="text-2xl font-black text-gata-green uppercase italic tracking-tight">1. Data We Collect</h2>
              <div class="space-y-4 text-gata-cream/60 leading-relaxed font-medium">
                <p>When you sign in using Google OAuth, we collect your:</p>
                <ul class="list-disc pl-5 space-y-2 marker:text-gata-green">
                  <li>Google Email Address</li>
                  <li>Basic Profile Information (Name, Profile Picture)</li>
                  <li>Authentication Tokens (to manage your secure session)</li>
                </ul>
                <p>We do <span class="text-gata-cream font-bold">not</span> collect or store any data from your private Google Drive or GCS buckets unless you explicitly connect them (feature currently disabled for demo).</p>
              </div>
            </section>

            <section class="space-y-4">
              <h2 class="text-2xl font-black text-gata-green uppercase italic tracking-tight">2. How We Use Your Information</h2>
              <div class="space-y-4 text-gata-cream/60 leading-relaxed font-medium">
                <p>Your information is used strictly for:</p>
                <ul class="list-disc pl-5 space-y-2 marker:text-gata-green">
                  <li>Identity Verification and Authentication.</li>
                  <li>Maintaining your personalized dashboard session.</li>
                  <li>Providing access to Smarter features and demo environments.</li>
                </ul>
              </div>
            </section>

            <section class="space-y-4">
              <h2 class="text-2xl font-black text-gata-green uppercase italic tracking-tight">3. Data Storage & Security</h2>
              <div class="space-y-4 text-gata-cream/60 leading-relaxed font-medium">
                <p>We use industry-standard encryption to protect your data. Your session info is stored in our distributed Deno KV database. We do not sell your data to third parties. Ever.</p>
              </div>
            </section>

            <section class="space-y-4">
              <h2 class="text-2xl font-black text-gata-green uppercase italic tracking-tight">4. Your Rights</h2>
              <div class="space-y-4 text-gata-cream/60 leading-relaxed font-medium">
                <p>You have the right to:</p>
                <ul class="list-disc pl-5 space-y-2 marker:text-gata-green">
                  <li>Request deletion of your account and associated data.</li>
                  <li>Withdraw your Google OAuth permissions at any time via your Google Account settings.</li>
                </ul>
              </div>
            </section>

            <section class="space-y-4 p-8 bg-gata-green/5 border border-gata-green/10 rounded-3xl">
              <h2 class="text-lg font-black text-gata-green uppercase italic tracking-tight">Contact Us</h2>
              <p class="text-sm text-gata-cream/60 font-medium">
                Questions? Reach out to our swamp-dwellers at: <br/>
                <a href="mailto:thedatagata@gmail.com" class="text-gata-cream hover:text-gata-green transition-colors font-black uppercase tracking-widest mt-2 block">thedatagata@gmail.com</a>
              </p>
            </section>
          </div>
        </div>
      </main>

      <GataFooter />
    </div>
  );
}
