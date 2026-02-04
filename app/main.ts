// main.ts
import { start } from "$fresh/server.ts";
import manifest from "./fresh.gen.ts";
import config from "./fresh.config.ts";

// Load .env file
await import("$std/dotenv/load.ts");

console.log("Environment Check:");
console.log("ADMIN_EMAIL present:", !!Deno.env.get("ADMIN_EMAIL"));
console.log("SESSION_SECRET present:", !!Deno.env.get("SESSION_SECRET"));

if (Deno.env.get("BUILD_PHASE") !== "true") {
  // Seed admin user on startup
  try {
    const { default: seedAdmin } = await import("./utils/scripts/seed_admin.ts");
    await seedAdmin();
  } catch (e) {
    console.error("Failed to seed admin:", e);
  }
}

await start(manifest, config);
