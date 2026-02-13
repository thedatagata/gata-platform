import { Handlers } from "$fresh/server.ts";
import { getUser, getUserByEmail } from "../../../utils/models/user.ts";
import { createSession } from "../../../utils/models/session.ts";
import * as bcrypt from "https://deno.land/x/bcrypt@v0.4.1/mod.ts";

export const handler: Handlers = {
  async POST(req) {
    try {
      const { username, password } = await req.json();

      if (!username || !password) {
        return new Response(JSON.stringify({ error: "Username and password required" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Try finding user by username or email
      let user = await getUser(username);
      if (!user && username.includes("@")) {
        user = await getUserByEmail(username);
      }

      if (!user) {
        console.log(` Login failed: User identified by '${username}' not found.`);
        return new Response(JSON.stringify({ error: "Invalid credentials" }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Check security status
      if (user.securityRestricted) {
        return new Response(JSON.stringify({ 
          error: "Account Restricted", 
          details: "Your Google Account reported a security event. Please contact support." 
        }), {
          status: 403,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Verify password
      if (!user.passwordHash) {
        return new Response(JSON.stringify({ error: "Account has no password (try Google login)" }), {
          status: 403,
          headers: { "Content-Type": "application/json" },
        });
      }

      const valid = await bcrypt.compare(password, user.passwordHash);
      if (!valid) {
        console.log(` Login failed: Password mismatch for user '${user.username}'.`);
        return new Response(JSON.stringify({ error: "Invalid credentials" }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Create session
      const session = await createSession(user.username);

      // Set cookie
      const headers = new Headers({
        "Content-Type": "application/json",
        "Set-Cookie": `session_id=${session.sessionId}; Path=/; HttpOnly; Max-Age=${7 * 24 * 60 * 60}; SameSite=Lax`,
      });

      return new Response(
        JSON.stringify({
          success: true,
          user: {
            username: user.username,
          },
        }),
        { headers }
      );
    } catch (error) {
      console.error("Login error:", error);
      return new Response(
        JSON.stringify({ error: "Login failed" }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  },
};
