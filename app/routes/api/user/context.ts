import { Handlers } from "$fresh/server.ts";
import { getSession } from "../../../utils/models/session.ts";
import { getUser } from "../../../utils/models/user.ts";

export const handler: Handlers = {
  async GET(req) {
    const url = new URL(req.url);
    const sessionId = url.searchParams.get("sessionId");

    if (!sessionId) {
      return new Response("Missing sessionId", { status: 400 });
    }

    try {
      // Get session to find username
      const session = await getSession(sessionId);
      
      if (!session) {
        // Return default context for anonymous users
        return new Response(JSON.stringify({
          plan_tier: "free",
          ai_addon_unlocked: false,
          ai_analyst_unlocked: false
        }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      // Get user data
      const user = await getUser(session.username);
      
      if (!user) {
        return new Response("User not found", { status: 404 });
      }

      // Return user context
      return new Response(JSON.stringify({
        plan_tier: user.plan_tier,
        ai_addon_unlocked: user.ai_addon_unlocked,
        ai_analyst_unlocked: user.ai_analyst_unlocked
      }), {
        headers: { "Content-Type": "application/json" },
      });
    } catch (error) {
      console.error("Failed to get user context:", error);
      return new Response("Internal Server Error", { status: 500 });
    }
  },
};
