import { createUser, getUser } from "../models/user.ts";
import * as bcrypt from "https://deno.land/x/bcrypt@v0.4.1/mod.ts";

export default async function seedAdmin() {
  const adminEmail = Deno.env.get("ADMIN_EMAIL");
  // Use SESSION_SECRET as the initial password for simplicity in this script, 
  // or a specific ADMIN_PASSWORD if you prefer. 
  // For now, I'll use a default if not provided to avoid crashing, but you should set it.
  // Hardcoding password as requested to ensure match
  const adminPassword = "a3f2b1c0-5d4e-4a1b-9c8d-7e6f5a4b3c2da3f2b1c0-5d4e-4a1b-9c8d-7e6f5a4b3c2d";

  if (!adminEmail) {
    console.error("❌ ADMIN_EMAIL not set in environment");
    return; // Don't exit, just return
  }

  console.log(`Checking for admin user: ${adminEmail}...`);
  
  const existing = await getUser(adminEmail);
  
  const salt = await bcrypt.genSalt(8);
  const hash = await bcrypt.hash(adminPassword, salt);

  if (existing) {
    console.log("🔄 Updating existing admin user...");
    // We can't use updateUser for password yet as it doesn't support it in the Partial
    // So we'll use createUser to overwrite (it uses set which overwrites)
    // But createUser checks for existence first... wait.
    
    // Let's modify createUser to allow overwrite or just use KV directly here for the seed script
    // Actually, let's just use the KV directly to update the password hash
    
    // IMPORT getKv to ensure we use the same DB path!
    const { getKv } = await import("../system/db.ts");
    const kv = await getKv();
    
    const key = ["users", adminEmail];
    const updatedUser = {
      ...existing,
      passwordHash: hash,
      plan_tier: "premium" as const,
      ai_addon_unlocked: true,
      ai_analyst_unlocked: true,
      preferred_model_tier: "7b" as const,
      updatedAt: new Date()
    };
    
    await kv.set(key, updatedUser);
    console.log("✅ Admin user updated with current credentials!");
    return;
  }

  console.log("Creating admin user...");
  
  await createUser(
    adminEmail,
    hash,
    "premium", // Give admin premium access
    true,      // Unlock AI
    true,      // Unlock Analyst
    "7b"       // Give admin the powerful model
  );

  console.log(`✅ Admin user created!`);
  console.log(`Username: ${adminEmail}`);
  console.log(`Password: (Your SESSION_SECRET)`);
}

if (import.meta.main) {
  await seedAdmin();
}
