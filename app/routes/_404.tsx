// routes/_404.tsx
import { Head } from "$fresh/runtime.ts";

export default function NotFoundPage() {
  return (
    <>
      <Head>
        <title>404 - Page Not Found | DATA_GATA</title>
      </Head>
      <div class="min-h-screen flex flex-col items-center justify-center bg-gata-dark text-gata-cream p-4">
        <div class="w-24 h-24 mb-6 relative">
          <img
            src="/gata_app_utils/nerdy_alligator_headshot.png"
            alt="DATA_GATA Logo"
            class="w-full h-full object-cover rounded-full border-4 border-gata-green"
          />
          <div class="absolute -bottom-2 -right-2 bg-gata-green text-gata-dark w-10 h-10 rounded-full flex items-center justify-center font-bold text-xl">
            ?
          </div>
        </div>

        <h1 class="text-4xl md:text-6xl font-bold mb-4">404</h1>
        <h2 class="text-2xl md:text-3xl font-light mb-8 text-center">
          Looks like you've wandered into the data swamp
        </h2>
        <p class="text-xl text-gata-cream/70 max-w-md text-center mb-8">
          The page you're looking for seems to have been lost in the murky waters. Let's get you back to solid ground.
        </p>
        <a
          href="/"
          class="px-6 py-3 bg-gata-green text-gata-dark font-medium rounded-md hover:bg-[#a0d147] transition-colors inline-flex items-center"
        >
          <svg class="w-4 h-4 mr-2 inline" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0a1 1 0 01-1-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 01-1 1" /></svg>
          Return to Homepage
        </a>
      </div>
    </>
  );
}
