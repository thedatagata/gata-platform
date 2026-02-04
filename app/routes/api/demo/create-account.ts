import { Handlers } from "$fresh/server.ts";
import { setCookie } from "$std/http/cookie.ts";
import { createUser } from "../../../utils/models/user.ts";
import { createSession } from "../../../utils/models/session.ts";
import * as bcrypt from "https://deno.land/x/bcrypt@v0.4.1/mod.ts";

export const handler: Handlers = {
  async POST(req) {
    try {
      const { username, password, plan, ai_addon_unlocked, ai_analyst_unlocked, demoEmail: _demoEmail } = await req.json();

      if (!username || !password || !plan) {
        return new Response(JSON.stringify({ error: "Missing fields" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Hash password
      const passwordHash = await bcrypt.hash(password);

      // Create dummy user
      // We store the demoEmail in the user record for tracking/analytics if needed
      // But the user context will be based on the dummy username
      const planTier = plan === 'smarter' ? 'premium' : 'free';
      // Strictly respect the add-on flags from the request
      const aiAddon = ai_addon_unlocked;
      const aiAnalyst = ai_analyst_unlocked;
      
      const _user = await createUser(
        username, 
        passwordHash, 
        planTier,
        aiAddon,
        aiAnalyst,
        '3b', // preferred_model_tier
        _demoEmail // demoEmail
      );

      // Create session
      const session = await createSession(username);

      // Set session cookie
      const headers = new Headers();
      headers.set("Content-Type", "application/json");
      setCookie(headers, {
        name: "session_id",
        value: session.sessionId,
        maxAge: 60 * 60 * 24 * 7, // 1 week
        path: "/",
        httpOnly: true,
        sameSite: "Lax",
      });

      return new Response(JSON.stringify({ success: true }), {
        status: 200,
        headers,
      });

    } catch (error) {
      console.error("Create demo account error:", error);
      return new Response(JSON.stringify({ error: error instanceof Error ? error.message : "Failed to create account" }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
};

