import { Head } from "$fresh/runtime.ts";
import Nav from "../components/Nav.tsx";
import GataFooter from "../components/GataFooter.tsx";
import HeroFeature from "../islands/HeroFeature.tsx";
import PlatformCapabilities from "../islands/PlatformCapabilities.tsx";
import ExperienceSection from "../islands/ExperienceSection.tsx";

export default function Home() {
  return (
    <div class="relative min-h-screen bg-gata-dark">
      <Head>
        <title>DATA_GATA | Modern Data Architecture</title>
      </Head>
      <Nav />

      <main>
        <HeroFeature />
        <PlatformCapabilities />
        <ExperienceSection />
      </main>

      <GataFooter />
    </div>
  );
}
