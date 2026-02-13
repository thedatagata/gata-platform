// Readiness check — proxies to Platform API (same pattern as all other routes)

import { Handlers } from "$fresh/server.ts";

const PLATFORM_API_URL = Deno.env.get("PLATFORM_API_URL") || "http://localhost:8001";

export const handler: Handlers = {
  async GET(_req, ctx) {
    const { tenant } = ctx.params;
    const targetUrl = `${PLATFORM_API_URL}/readiness/${tenant}`;

    try {
      const response = await fetch(targetUrl, {
        headers: { "Content-Type": "application/json" },
      });
      const data = await response.text();
      return new Response(data, {
        status: response.status,
        headers: { "Content-Type": "application/json" },
      });
    } catch (error) {
      console.error(`Readiness proxy error for ${tenant}:`, error);
      return new Response(JSON.stringify({
        is_ready: false,
        last_load_id: null,
        status: "error",
        message: "Platform API unavailable",
      }), {
        status: 502,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
};
