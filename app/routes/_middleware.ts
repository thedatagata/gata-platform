import { MiddlewareHandlerContext } from "$fresh/server.ts";
import { getSession, deleteSession } from "../utils/models/session.ts";
import { getUser } from "../utils/models/user.ts";

export async function handler(
  req: Request,
  ctx: MiddlewareHandlerContext,
) {
  // Parse session_id from cookies
  const cookies = req.headers.get("cookie");
  console.log("Middleware Cookies:", cookies);
  
  const sessionId = cookies
    ?.split(";")
    .find((c) => c.trim().startsWith("session_id="))
    ?.split("=")[1];

  console.log("Middleware Parsed SessionId:", sessionId);

  if (sessionId) {
    ctx.state.sessionId = sessionId;
    
    // background security check
    const session = await getSession(sessionId);
    if (session) {
      const user = await getUser(session.username);
      if (user?.securityRestricted) {
        console.warn(`🛑 [Security] Blocking session for restricted user: ${user.username}`);
        await deleteSession(sessionId);
        return new Response("Account Restricted", { status: 403 });
      }
    }
  }

  const resp = await ctx.next();
  
  // Required for MotherDuck WASM (SharedArrayBuffer) - both dashboards use it
  resp.headers.set("Cross-Origin-Opener-Policy", "same-origin");
  resp.headers.set("Cross-Origin-Embedder-Policy", "require-corp");
  
  return resp;
}
