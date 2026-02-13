import { Handlers } from "$fresh/server.ts";
import { getSession } from "../../../utils/models/session.ts";
import { getUser } from "../../../utils/models/user.ts";

export const handler: Handlers = {
  async GET(req) {
    try {
      // Get session from cookie
      const cookies = req.headers.get("cookie");
      const sessionId = cookies
        ?.split(";")
        .find((c) => c.trim().startsWith("session_id="))
        ?.split("=")[1];

      if (!sessionId) {
        return new Response(JSON.stringify({ authenticated: false }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Validate session
      const session = await getSession(sessionId);
      if (!session) {
        return new Response(JSON.stringify({ authenticated: false }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Get user
      const user = await getUser(session.username);
      if (!user) {
        return new Response(JSON.stringify({ authenticated: false }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        });
      }

      return new Response(
        JSON.stringify({
          authenticated: true,
          user: {
            username: user.username,
          },
        }),
        {
          headers: { "Content-Type": "application/json" },
        }
      );
    } catch (error) {
      console.error("Auth check error:", error);
      return new Response(JSON.stringify({ authenticated: false }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
};
