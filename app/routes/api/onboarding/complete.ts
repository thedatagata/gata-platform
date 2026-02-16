import { Handlers } from "$fresh/server.ts";
import { getSession } from "../../../utils/models/session.ts";
import { updateUser } from "../../../utils/models/user.ts";

const PLATFORM_API_URL = Deno.env.get("PLATFORM_API_URL") || "http://localhost:8001";

export const handler: Handlers = {
  async POST(req, _ctx) {
    try {
      const { tenant_slug, business_name, sources } = await req.json();

      if (!tenant_slug || !business_name) {
        return new Response(JSON.stringify({ error: "Missing required fields" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      // 1. Call platform-api to register tenant and start pipeline
      const apiRes = await fetch(`${PLATFORM_API_URL}/onboard`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tenant_slug, business_name, sources }),
      });

      if (!apiRes.ok) {
        const err = await apiRes.text();
        console.error(`[ONBOARD] Platform API error: ${err}`);
        return new Response(
          JSON.stringify({ error: "Failed to start onboarding pipeline" }),
          { status: 502, headers: { "Content-Type": "application/json" } },
        );
      }

      const apiData = await apiRes.json();

      // 2. Set tenant_slug on the logged-in user (Deno KV)
      const cookies = req.headers.get("cookie");
      const sessionId = cookies
        ?.split(";")
        .find((c) => c.trim().startsWith("session_id="))
        ?.split("=")[1];
      if (sessionId) {
        const session = await getSession(sessionId);
        if (session) {
          await updateUser(session.username, { tenant_slug: tenant_slug });
        }
      }

      return new Response(
        JSON.stringify({ success: true, ...apiData }),
        { status: 202, headers: { "Content-Type": "application/json" } },
      );

    } catch (error) {
      console.error("Onboarding error:", error);
      return new Response(
        JSON.stringify({ error: "Failed to complete onboarding" }),
        { status: 500, headers: { "Content-Type": "application/json" } },
      );
    }
  },
};
