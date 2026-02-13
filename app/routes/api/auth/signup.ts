import { Handlers } from "$fresh/server.ts";
import { createUser } from "../../../utils/models/user.ts";
import { createSession } from "../../../utils/models/session.ts";
import * as bcrypt from "https://deno.land/x/bcrypt@v0.4.1/mod.ts";

export const handler: Handlers = {
  async POST(req) {
    try {
      const { username, password, email } = await req.json();

      if (!username || !password) {
        return new Response(JSON.stringify({ error: "Username and password required" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Validate username (alphanumeric, 3-20 chars)
      if (!/^[a-zA-Z0-9_]{3,20}$/.test(username)) {
        return new Response(JSON.stringify({ error: "Invalid username format" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Hash password
      const passwordHash = await bcrypt.hash(password);

      // Create user
      const user = await createUser(username, {
        passwordHash,
        email,
      });

      // Create session
      const session = await createSession(username);

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
      console.error("Signup error:", error);
      return new Response(
        JSON.stringify({ error: (error as Error).message || "Signup failed" }),
        {
          status: 400,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  },
};
