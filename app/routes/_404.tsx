// routes/_404.tsx
import { Head } from "$fresh/runtime.ts";

export default function NotFoundPage() {
  return (
    <>
      <Head>
        <title>404 - Page Not Found | DATA_GATA</title>
      </Head>
      <div class="min-h-screen flex flex-col items-center justify-center bg-[#172217] text-[#F8F6F0] p-4">
        <div class="w-24 h-24 mb-6 relative">
          <img
            src="/gata_app_utils/nerdy_alligator_headshot.png"
            alt="DATA_GATA Logo"
            class="w-full h-full object-cover rounded-full border-4 border-[#90C137]"
          />
          <div class="absolute -bottom-2 -right-2 bg-[#90C137] text-[#172217] w-10 h-10 rounded-full flex items-center justify-center font-bold text-xl">
            ?
          </div>
        </div>
        
        <h1 class="text-4xl md:text-6xl font-bold mb-4">404</h1>
        <h2 class="text-2xl md:text-3xl font-light mb-8 text-center">
          Looks like you've wandered into the data swamp
        </h2>
        <p class="text-xl text-[#F8F6F0]/70 max-w-md text-center mb-8">
          The page you're looking for seems to have been lost in the murky waters. Let's get you back to solid ground.
        </p>
        <a 
          href="/" 
          class="px-6 py-3 bg-[#90C137] text-[#172217] font-medium rounded-md hover:bg-[#a0d147] transition-colors inline-flex items-center"
        >
          <i class="fas fa-home mr-2"></i>
          Return to Homepage
        </a>
      </div>
    </>
  );
}