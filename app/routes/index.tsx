import { Handlers, PageProps } from "$fresh/server.ts";
import Nav from "../components/Nav.tsx";
import GataFooter from "../components/GataFooter.tsx";
import HeroFeature from "../islands/HeroFeature.tsx";
import ExperienceSection from "../islands/ExperienceSection.tsx";
import ContextSwitcher from "../islands/ContextSwitcher.tsx";

interface LandingData {
  ldClientId?: string;
}

export const handler: Handlers<LandingData> = {
  GET(_req, ctx) {
    const ldClientId = Deno.env.get("LAUNCHDARKLY_CLIENT_ID");
    return ctx.render({ ldClientId });
  },
};

export default function Home({ data }: PageProps<LandingData>) {
  const { ldClientId } = data;

  return (
    <div class="relative min-h-screen bg-gata-dark">
      <Nav />
      
      <main>
        <HeroFeature _ldClientId={ldClientId} />
        
        {/* Track Record Section */}
        <ExperienceSection />
      </main>

      <GataFooter />

      {/* Debug Tool for LaunchDarkly Experiments */}
      {ldClientId && <ContextSwitcher ldClientId={ldClientId} />}
    </div>
  );
}