import { getKv } from "../system/db.ts";

export interface User {
  username: string;
  email?: string;
  googleId?: string;
  passwordHash?: string; // Optional if using Google OAuth
  plan_tier: "free" | "premium";
  ai_addon_unlocked: boolean;
  ai_analyst_unlocked: boolean;
  preferred_model_tier?: "3b" | "7b";
  motherDuckToken?: string;
  googleAccessToken?: string;
  googleRefreshToken?: string;
  demoEmail?: string;
  securityRestricted?: boolean;
  lastSecurityEvent?: string;
  createdAt: Date;
  updatedAt: Date;
}

export async function createUser(
  username: string, 
  data: {
    email?: string;
    googleId?: string;
    passwordHash?: string;
    plan_tier?: "free" | "premium";
    ai_addon_unlocked?: boolean;
    ai_analyst_unlocked?: boolean;
    preferred_model_tier?: "3b" | "7b";
    demoEmail?: string;
  }
): Promise<User> {
  const kv = await getKv();
  const userKey = ["users", username];
  
  const user: User = {
    username,
    email: data.email,
    googleId: data.googleId,
    passwordHash: data.passwordHash,
    plan_tier: data.plan_tier || "free",
    ai_addon_unlocked: data.ai_addon_unlocked || false,
    ai_analyst_unlocked: data.ai_analyst_unlocked || false,
    preferred_model_tier: data.preferred_model_tier || "3b",
    demoEmail: data.demoEmail,
    securityRestricted: false,
    createdAt: new Date(),
    updatedAt: new Date()
  };
  
  const atomic = kv.atomic().check({ key: userKey, versionstamp: null });

  // Add index for email if provided
  if (data.email) {
    const emailKey = ["users_by_email", data.email];
    atomic.check({ key: emailKey, versionstamp: null });
    atomic.set(emailKey, username);
  }

  // Add index for googleId if provided
  if (data.googleId) {
    const googleIdKey = ["users_by_google", data.googleId];
    atomic.check({ key: googleIdKey, versionstamp: null });
    atomic.set(googleIdKey, username);
  }

  const res = await atomic
    .set(userKey, user)
    .commit();

  if (!res.ok) {
    throw new Error("Username, email, or Google account already exists");
  }
  
  console.log(`👤 Created user: ${username}`);
  return user;
}

export async function getUser(username: string): Promise<User | null> {
  const kv = await getKv();
  const result = await kv.get<User>(["users", username]);
  return result.value;
}

export async function getUserByEmail(email: string): Promise<User | null> {
  const kv = await getKv();
  const index = await kv.get<string>(["users_by_email", email]);
  if (!index.value) return null;
  return getUser(index.value);
}

export async function getUserByGoogleId(googleId: string): Promise<User | null> {
  const kv = await getKv();
  const index = await kv.get<string>(["users_by_google", googleId]);
  if (!index.value) return null;
  return getUser(index.value);
}

export async function updateUser(username: string, updates: Partial<User>): Promise<void> {
  const kv = await getKv();
  const key = ["users", username];
  
  let committed = false;
  while (!committed) {
    const entry = await kv.get<User>(key);
    if (!entry.value) {
      throw new Error("User not found");
    }
    
    const updated: User = {
      ...entry.value,
      ...updates,
      updatedAt: new Date()
    };
    
    const result = await kv.atomic()
      .check(entry)
      .set(key, updated)
      .commit();
      
    committed = result.ok;
  }
  
  console.log(`💾 Updated user: ${username}`, updates);
}

export async function listAllUsers(): Promise<User[]> {
  const kv = await getKv();
  const users: User[] = [];
  
  const entries = kv.list<User>({ prefix: ["users"] });
  for await (const entry of entries) {
    users.push(entry.value);
  }
  
  return users;
}

export async function deleteUser(username: string): Promise<void> {
  const kv = await getKv();
  const user = await getUser(username);
  if (!user) return;

  const atomic = kv.atomic().delete(["users", username]);
  if (user.email) atomic.delete(["users_by_email", user.email]);
  if (user.googleId) atomic.delete(["users_by_google", user.googleId]);
  
  await atomic.commit();
  console.log(`🗑️ Deleted user: ${username}`);
}


