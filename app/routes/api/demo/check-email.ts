import { Handlers } from "$fresh/server.ts";
import { getVariation } from "../../../utils/launchdarkly/server.ts";
import { buildUserContext } from "../../../utils/launchdarkly/context-builder.ts";
import { FLAGS } from "../../../utils/launchdarkly/flags.ts";

export const handler: Handlers = {
  async POST(req) {
    try {
      const { email } = await req.json();

      if (!email) {
        return new Response(JSON.stringify({ error: "Email required" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      // Check LaunchDarkly Allowlist
      const context = buildUserContext(email);
      const isAllowed = await getVariation(context, FLAGS.DEMO_ACCESS_ALLOWLIST, false);

      if (!isAllowed) {
        return new Response(JSON.stringify({ error: "Email not in allowlist. Please request access." }), {
          status: 403,
          headers: { "Content-Type": "application/json" },
        });
      }

      return new Response(JSON.stringify({ success: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });

    } catch (error) {
      console.error("Check email error:", error);
      return new Response(JSON.stringify({ error: "Internal server error" }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
};
