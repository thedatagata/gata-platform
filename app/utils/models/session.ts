import { getKv } from "../system/db.ts";

export interface Session {
  sessionId: string;
  username: string;
  expiresAt: Date;
}

export async function createSession(username: string): Promise<Session> {
  const kv = await getKv();
  
  const sessionId = crypto.randomUUID();
  const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000); // 7 days
  
  const session: Session = {
    sessionId,
    username,
    expiresAt
  };
  
  // Atomic set to ensure we don't overwrite (extremely unlikely with UUID)
  const res = await kv.atomic()
    .check({ key: ["sessions", sessionId], versionstamp: null })
    .set(["sessions", sessionId], session)
    .commit();

  if (!res.ok) {
    // Retry once if collision (extremely rare)
    return createSession(username);
  }
  
  console.log(`🔐 Created session for ${username}: ${sessionId}`);
  return session;
}

export async function getSession(sessionId: string): Promise<Session | null> {
  const kv = await getKv();
  const result = await kv.get<Session>(["sessions", sessionId]);
  
  if (!result.value) return null;
  
  // Check if expired
  if (new Date(result.value.expiresAt) < new Date()) {
    await kv.delete(["sessions", sessionId]);
    return null;
  }
  
  return result.value;
}

export async function deleteSession(sessionId: string): Promise<void> {
  const kv = await getKv();
  await kv.delete(["sessions", sessionId]);
  console.log(`🔓 Deleted session: ${sessionId}`);
}
export async function deleteSessionsForUser(username: string): Promise<void> {
  const kv = await getKv();
  const entries = kv.list<Session>({ prefix: ["sessions"] });
  const atomic = kv.atomic();
  
  let count = 0;
  for await (const entry of entries) {
    if (entry.value.username === username) {
      atomic.delete(entry.key);
      count++;
    }
  }
  
  if (count > 0) {
    await atomic.commit();
    console.log(`🧹 Revoked ${count} sessions for user: ${username}`);
  }
}
