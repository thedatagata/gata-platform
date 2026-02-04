import { Handlers } from "$fresh/server.ts";
import { getSession } from "../../../utils/models/session.ts";
import { getUser } from "../../../utils/models/user.ts";
import { createDemoAccessUser, deleteDemoAccessUser, listDemoAccessUsers } from "../../../utils/models/demo_access.ts";

export const handler: Handlers = {
  async GET(req, ctx) {
    // Auth check
    const sessionId = ctx.state.sessionId as string;
    if (!sessionId) return new Response("Unauthorized", { status: 401 });
    const session = await getSession(sessionId);
    if (!session) return new Response("Unauthorized", { status: 401 });
    const admin = await getUser(session.username);
    if (!admin) return new Response("Unauthorized", { status: 401 });

    const users = await listDemoAccessUsers();
    // Don't send hashes
    const safeUsers = users.map(u => ({ email: u.email, createdAt: u.createdAt }));
    
    return new Response(JSON.stringify(safeUsers), {
      headers: { "Content-Type": "application/json" }
    });
  },

  async POST(req, ctx) {
    // Auth check
    const sessionId = ctx.state.sessionId as string;
    if (!sessionId) return new Response("Unauthorized", { status: 401 });
    const session = await getSession(sessionId);
    if (!session) return new Response("Unauthorized", { status: 401 });
    const admin = await getUser(session.username);
    if (!admin) return new Response("Unauthorized", { status: 401 });

    const { email, password, action } = await req.json();

    if (action === "delete") {
      if (!email) return new Response("Email required", { status: 400 });
      await deleteDemoAccessUser(email);
      return new Response(JSON.stringify({ success: true }));
    }

    if (action === "create") {
      if (!email || !password) return new Response("Email and password required", { status: 400 });
      await createDemoAccessUser(email, password);
      return new Response(JSON.stringify({ success: true }));
    }

    return new Response("Invalid action", { status: 400 });
  }
};
