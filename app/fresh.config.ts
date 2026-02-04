// fresh.config.ts
import { defineConfig } from "$fresh/server.ts";
import twindPlugin from "$fresh/plugins/twind.ts";
import twindConfig from "./twind.config.ts";

export default defineConfig({
  plugins: [twindPlugin(twindConfig)],
  server: {
    // Enable COOP/COEP headers for SharedArrayBuffer (WebGPU performance)
    onListen: ({ hostname, port }) => {
      console.log(`🦎 Server running at http://${hostname}:${port}`);
      console.log(`⚡ WebGPU enabled with COOP/COEP headers`);
    },
  },
});