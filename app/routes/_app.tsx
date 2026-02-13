// routes/_app.tsx
import { PageProps } from "$fresh/server.ts";
import { Head } from "$fresh/runtime.ts";
import ScrollToTop from "../islands/app_utils/ScrollToTop.tsx";
import ToastContainer from "../islands/app_utils/Toast.tsx";

export default function App({ Component }: PageProps) {
  return (
    <>
      <Head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>DATA_GATA | Modern Data Architecture</title>
        <meta name="description" content="DATA_GATA LLC provides expert data architecture, analytics engineering, and lakehouse implementation services." />
        <link rel="icon" href="/gata_app_utils/nerdy_alligator_headshot.png" type="image/png" />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet" />
        <link rel="stylesheet" href="/gata_app_utils/styles.css" />
      </Head>
      <div class="font-sans bg-gata-cream min-h-screen flex flex-col">
        <Component />
        <ScrollToTop />
        <ToastContainer />
      </div>
    </>
  );
}