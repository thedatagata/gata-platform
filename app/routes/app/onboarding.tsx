import { PageProps } from "$fresh/server.ts";
import { Head } from "$fresh/runtime.ts";
import SourceSelector from "../../islands/onboarding/SourceSelector.tsx";

export default function OnboardingPage(props: PageProps) {
  const { url } = props;
  const tenantSlug = url.searchParams.get("tenant") || "demo_tenant";
  const companyName = url.searchParams.get("company") || "Demo Company";

  return (
    <div class="min-h-screen bg-gata-dark relative overflow-hidden flex flex-col items-center justify-center p-4">
      <Head><title>Setup Your Stack | DATA_GATA</title></Head>
      {/* Background Ambience */}
      <div class="fixed inset-0 pointer-events-none">
         <div class="absolute top-[-20%] left-[20%] w-[60%] h-[60%] bg-gata-green/5 rounded-full blur-[150px] animate-pulse" />
      </div>

      <div class="relative z-10 w-full">
         <div class="mb-10 text-center">
            <div class="inline-block p-2 border border-gata-green/20 rounded-2xl bg-gata-dark/50 backdrop-blur-md mb-4">
               <img src="/logo.png" class="h-8 w-auto mix-blend-screen opacity-80" alt="GATA" />
            </div>
         </div>
         <SourceSelector tenantSlug={tenantSlug} companyName={companyName} />
      </div>
    </div>
  );
}
