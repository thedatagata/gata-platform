import { Handlers } from "$fresh/server.ts";

export const handler: Handlers = {
  GET() {
    const token = Deno.env.get("MOTHERDUCK_TOKEN") || "";
    return new Response(JSON.stringify({ token }), {
      headers: { "Content-Type": "application/json" },
    });
  },
};
