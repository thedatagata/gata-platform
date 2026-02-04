import { Handlers } from "$fresh/server.ts";
import { getSession } from "../../../utils/models/session.ts";
import { getUser, createUser } from "../../../utils/models/user.ts";
import * as bcrypt from "https://deno.land/x/bcrypt@v0.4.1/mod.ts";

export const handler: Handlers = {
  async POST(req, ctx) {
    try {
      // 1. Check admin auth
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

      // 2. Get request data
      const { email, tempPassword, modelTier } = await req.json();

      if (!email || !tempPassword) {
        return new Response(JSON.stringify({ error: "Email and password required" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Validate modelTier
      const tier = (modelTier === "7b" ? "7b" : "3b") as "3b" | "7b";

      // 3. Check if user already exists
      const existing = await getUser(email);
      if (existing) {
        return new Response(JSON.stringify({ error: "User already exists" }), {
          status: 409,
          headers: { "Content-Type": "application/json" },
        });
      }

      // 4. Create user
      const salt = await bcrypt.genSalt(8);
      const hash = await bcrypt.hash(tempPassword, salt);

      await createUser(
        email,
        hash,
        "free", // Default to free tier
        false,  // No AI addon
        false,  // No analyst addon
        tier    // Model tier from admin selection
      );

      return new Response(
        JSON.stringify({
          success: true,
          message: `User ${email} created successfully`,
        }),
        {
          status: 201,
          headers: { "Content-Type": "application/json" },
        }
      );
    } catch (error) {
      console.error("Create user error:", error);
      return new Response(
        JSON.stringify({ error: "Failed to create user" }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  },
};
