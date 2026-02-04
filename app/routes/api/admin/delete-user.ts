import { Handlers } from "$fresh/server.ts";
import { getSession } from "../../../utils/models/session.ts";
import { deleteUser } from "../../../utils/models/user.ts";

export const handler: Handlers = {
  async POST(req, ctx) {
    try {
      // Check admin auth
      const sessionId = (ctx.state as any).sessionId as string | undefined;
      if (!sessionId) {
        return new Response(JSON.stringify({ error: "Unauthorized" }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        });
      }

      const session = await getSession(sessionId);
      if (!session) {
        return new Response(JSON.stringify({ error: "Unauthorized" }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        });
      }

      const adminEmail = Deno.env.get("ADMIN_EMAIL");
      if (session.username !== adminEmail) {
        return new Response(JSON.stringify({ error: "Admin access required" }), {
          status: 403,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Get username to delete
      const { username } = await req.json();

      if (!username) {
        return new Response(JSON.stringify({ error: "Username required" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Don't allow admin to delete themselves
      if (username === adminEmail) {
        return new Response(JSON.stringify({ error: "Cannot delete admin account" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      await deleteUser(username);

      return new Response(
        JSON.stringify({
          success: true,
          message: `User ${username} deleted successfully`,
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }
      );
    } catch (error) {
      console.error("Delete user error:", error);
      return new Response(
        JSON.stringify({ error: "Failed to delete user" }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  },
};
