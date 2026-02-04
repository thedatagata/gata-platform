import { Handlers } from "$fresh/server.ts";
import { getKv } from "../../../utils/system/db.ts";

export const handler: Handlers = {
  async POST(req, ctx) {
    try {
      const { plan } = await req.json();
      
      if (!plan || (plan !== "base" && plan !== "premium")) {
        return new Response(
          JSON.stringify({ error: "Invalid plan" }),
          { status: 400, headers: { "Content-Type": "application/json" } }
        );
      }

      const sessionId = ctx.state.sessionId as string;
      const kv = await getKv();
      
      await kv.set(["user_plan", sessionId], {
        plan: plan as "base" | "premium",
        createdAt: new Date().toISOString()
      });

      return new Response(
        JSON.stringify({ success: true, plan }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
      
    } catch (error) {
      console.error("Onboarding error:", error);
      return new Response(
        JSON.stringify({ error: "Failed to complete onboarding" }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
  }
};
