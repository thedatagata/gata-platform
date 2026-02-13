import { Handlers } from "$fresh/server.ts";
import { parse, stringify } from "$std/yaml/mod.ts";
import { resolve } from "$std/path/mod.ts";
import { getSession } from "../../../utils/models/session.ts";
import { updateUser } from "../../../utils/models/user.ts";

interface Tenant {
  slug: string;
  business_name: string;
  status: string;
  sources: Record<string, unknown>;
}

interface TenantsConfig {
  tenants: Tenant[];
}

export const handler: Handlers = {
  async POST(req, _ctx) {
    try {
      const { tenant_slug, business_name, sources } = await req.json();

      if (!tenant_slug || !business_name) {
        return new Response(JSON.stringify({ error: "Missing required fields" }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      // 1. Locate and Read tenants.yaml
      // Deno.cwd() is likely 'app/', so we go up one level
      const tenantsPath = resolve(Deno.cwd(), "../tenants.yaml");
      const yamlContent = await Deno.readTextFile(tenantsPath);
      const config = parse(yamlContent) as TenantsConfig;

      // 2. Check overlap
      const exists = config.tenants.find((t) => t.slug === tenant_slug);
      if (exists) {
        // For now, accept it (idempotent-ish) or update? 
        // User flow implies new tenant. We will update specific fields if exists or skip.
        console.log(`[WARN] Tenant ${tenant_slug} already exists. Updating/Overwriting sources.`);
        exists.sources = sources; // Updating sources
      } else {
        // 3. Append new tenant
        config.tenants.push({
          slug: tenant_slug,
          business_name,
          status: "onboarding",
          sources: sources
        });
      }

      // 4. Write back
      await Deno.writeTextFile(tenantsPath, stringify(config as unknown as Record<string, unknown>));

      // 4b. Set tenant_slug on the logged-in user
      const cookies = req.headers.get("cookie");
      const sessionId = cookies
        ?.split(";")
        .find((c) => c.trim().startsWith("session_id="))
        ?.split("=")[1];
      if (sessionId) {
        const session = await getSession(sessionId);
        if (session) {
          await updateUser(session.username, { tenant_slug: tenant_slug });
        }
      }

      // 5. Trigger onboarding script asynchronously (includes dbt runs)
      const scriptPath = resolve(Deno.cwd(), "../scripts/onboard_tenant.py");

      const command = new Deno.Command("uv", {
        args: ["run", "python", scriptPath, tenant_slug, "--target", "dev", "--days", "30"],
        stdout: "inherit",
        stderr: "inherit",
      });

      // Fire-and-forget: the script generates scaffolding then runs dbt.
      // The frontend polls the readiness endpoint to know when it's done.
      const process = command.spawn();

      process.status.then((status) => {
        console.log(`[ONBOARD] Pipeline for ${tenant_slug} exited with code ${status.code}`);
      });

      return new Response(
        JSON.stringify({ success: true, message: "Tenant provisioning queued" }),
        { status: 202, headers: { "Content-Type": "application/json" } }
      );
      
    } catch (error) {
      console.error("Onboarding error:", error);
      return new Response(
        JSON.stringify({ error: "Failed to provision tenant" }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
  }
};
