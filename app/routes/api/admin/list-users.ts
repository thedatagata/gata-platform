import { Handlers } from "$fresh/server.ts";
import { getSession } from "../../../utils/models/session.ts";
import { listAllUsers } from "../../../utils/models/user.ts";

export const handler: Handlers = {
  async GET(_req, ctx) {
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

      // Get all users
      const users = await listAllUsers();
      
      // Don't send password hashes to client
      const sanitizedUsers = users.map(user => ({
        username: user.username,
        tenant_slug: user.tenant_slug,
        createdAt: user.createdAt,
      }));

      return new Response(JSON.stringify({ users: sanitizedUsers }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch (error) {
      console.error("List users error:", error);
      return new Response(
        JSON.stringify({ error: "Failed to list users" }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  },
};
