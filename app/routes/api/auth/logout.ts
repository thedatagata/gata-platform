import { Handlers } from "$fresh/server.ts";
import { deleteSession } from "../../../utils/models/session.ts";

export const handler: Handlers = {
  async POST(req) {
    try {
      // Get session from cookie
      const cookies = req.headers.get("cookie");
      const sessionId = cookies
        ?.split(";")
        .find((c) => c.trim().startsWith("session_id="))
        ?.split("=")[1];

      if (sessionId) {
        await deleteSession(sessionId);
      }

      // Clear cookie
      const headers = new Headers({
        "Content-Type": "application/json",
        "Set-Cookie": `session_id=; Path=/; HttpOnly; Max-Age=0; SameSite=Lax`,
      });

      return new Response(
        JSON.stringify({ success: true }),
        { headers }
      );
    } catch (error) {
      console.error("Logout error:", error);
      return new Response(
        JSON.stringify({ error: "Logout failed" }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  },
};
