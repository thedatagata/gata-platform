#!/usr/bin/env -S deno run -A --watch=static/,routes/

if (Deno.env.get("BUILD_PHASE") !== "true") {
  await import("$std/dotenv/load.ts");
}

import dev from "$fresh/dev.ts";
import config from "./fresh.config.ts";

// Seed admin user on startup in dev mode
if (Deno.env.get("BUILD_PHASE") !== "true") {
  try {
    const { default: seedAdmin } = await import("./utils/scripts/seed_admin.ts");
    await seedAdmin();
  } catch (e) {
    console.error("Failed to seed admin:", e);
  }
}

await dev(import.meta.url, "./main.ts", config);