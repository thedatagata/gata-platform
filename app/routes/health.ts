// routes/health.ts
import { Handlers } from "$fresh/server.ts";

export const handler: Handlers = {
  GET(_req) {
    return new Response(
      JSON.stringify({
        status: "healthy",
        timestamp: new Date().toISOString(),
        service: "thedenogatar"
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" }
      }
    );
  }
};