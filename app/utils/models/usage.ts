import { getKv } from "../system/db.ts";

export interface UserUsage {
  email: string;
  queryCount: number;
  lastReset: Date;
}

export async function getUsage(email: string): Promise<UserUsage> {
  const kv = await getKv();
  const result = await kv.get<UserUsage>(["usage", email]);
  
  if (!result.value) {
    return {
      email,
      queryCount: 0,
      lastReset: new Date()
    };
  }
  
  return result.value;
}

export async function incrementUsage(email: string): Promise<UserUsage> {
  const kv = await getKv();
  const key = ["usage", email];
  
  let committed = false;
  let currentUsage: UserUsage = { email, queryCount: 0, lastReset: new Date() };
  
  while (!committed) {
    const entry = await kv.get<UserUsage>(key);
    const value = entry.value || { email, queryCount: 0, lastReset: new Date() };
    
    const updated = {
      ...value,
      queryCount: value.queryCount + 1,
      lastReset: value.lastReset // We could implement daily reset logic here if needed
    };
    
    const result = await kv.atomic()
      .check(entry)
      .set(key, updated)
      .commit();
      
    if (result.ok) {
      committed = true;
      currentUsage = updated;
    }
  }
  
  return currentUsage;
}

export async function resetUsage(email: string): Promise<void> {
  const kv = await getKv();
  await kv.delete(["usage", email]);
}
