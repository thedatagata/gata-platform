// Catch-all proxy route — forwards requests to the FastAPI backend.
// Eliminates CORS issues by proxying through the Deno server.

import { Handlers } from "$fresh/server.ts";

const PLATFORM_API_URL = Deno.env.get("PLATFORM_API_URL") || "http://localhost:8001";

export const handler: Handlers = {
  async GET(req, ctx) {
    const path = ctx.params.path;
    const url = new URL(req.url);
    const targetUrl = `${PLATFORM_API_URL}/${path}${url.search}`;

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
      console.error(`Platform API proxy error: ${error}`);
      return new Response(JSON.stringify({ error: "Platform API unavailable" }), {
        status: 502,
        headers: { "Content-Type": "application/json" },
      });
    }
  },

  async POST(req, ctx) {
    const path = ctx.params.path;
    const url = new URL(req.url);
    const targetUrl = `${PLATFORM_API_URL}/${path}${url.search}`;
    const body = await req.text();

    try {
      const response = await fetch(targetUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
      });
      const data = await response.text();
      return new Response(data, {
        status: response.status,
        headers: { "Content-Type": "application/json" },
      });
    } catch (error) {
      console.error(`Platform API proxy error: ${error}`);
      return new Response(JSON.stringify({ error: "Platform API unavailable" }), {
        status: 502,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
};
