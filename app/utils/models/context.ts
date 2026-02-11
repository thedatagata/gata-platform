import { getKv } from "../system/db.ts";

export interface UserContext {
  sessionId: string;
  plan_tier: "free" | "premium";
  ai_addon_unlocked: boolean;
  ai_analyst_unlocked: boolean;
  createdAt: Date;
  updatedAt: Date;
}

export async function getUserContext(sessionId: string): Promise<UserContext> {
  const kv = await getKv();
  const result = await kv.get<UserContext>(["userContexts", sessionId]);
  
  if (result.value) {
    return result.value;
  }
  
  const defaultContext: UserContext = {
    sessionId,
    plan_tier: "free",
    ai_addon_unlocked: false,
    ai_analyst_unlocked: false,
    createdAt: new Date(),
    updatedAt: new Date()
  };
  
  // Atomic creation if not exists
  const res = await kv.atomic()
    .check({ key: ["userContexts", sessionId], versionstamp: null })
    .set(["userContexts", sessionId], defaultContext)
    .commit();

  if (!res.ok) {
    // If it was created in the meantime, fetch it again
    return getUserContext(sessionId);
  }
  
  return defaultContext;
}

export async function updateUserContext(
  sessionId: string, 
  updates: Partial<Pick<UserContext, "plan_tier" | "ai_addon_unlocked" | "ai_analyst_unlocked">>
): Promise<void> {
  const kv = await getKv();
  const key = ["userContexts", sessionId];
  
  let committed = false;
  while (!committed) {
    const entry = await kv.get<UserContext>(key);
    
    let current = entry.value;
    let versionstamp = entry.versionstamp;
    
    if (!current) {
      // If it doesn't exist, we create it with updates applied to default
      current = {
        sessionId,
        plan_tier: "free",
        ai_addon_unlocked: false,
        ai_analyst_unlocked: false,
        createdAt: new Date(),
        updatedAt: new Date()
      };
      versionstamp = null;
    }
    
    const updated: UserContext = {
      ...current,
      ...updates,
      updatedAt: new Date()
    };
    
    const result = await kv.atomic()
      .check({ key, versionstamp })
      .set(key, updated)
      .commit();
      
    committed = result.ok;
  }
  
  console.log(` DB: Updated context for ${sessionId}:`, updates);
}
