import { Head } from "$fresh/runtime.ts";

// Placeholder for future SmartDashboard with WebLLM
export default function SmartDashboardLoading() {
  return (
    <>
      <Head>
        <title>Smart Dashboard - Coming Soon</title>
      </Head>
      <div class="min-h-screen flex items-center justify-center bg-gradient-to-br from-[#172217] to-[#186018]">
        <div class="text-center">
          <h1 class="text-2xl font-bold text-[#F8F6F0] mb-4">
            Smart Dashboard Coming Soon
          </h1>
          <p class="text-[#F8F6F0]/70 mb-8">
            WebLLM + Semantic Layer integration in development
          </p>
          <a 
            href="/app/dashboard"
            class="inline-block px-6 py-3 bg-[#90C137] text-[#172217] font-semibold rounded-md hover:bg-[#a0d147] transition-colors"
          >
            Try Base Dashboard
          </a>
        </div>
      </div>
    </>
  );
}
