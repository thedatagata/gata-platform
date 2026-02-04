import { getKv } from "../system/db.ts";
import * as bcrypt from "https://deno.land/x/bcrypt@v0.4.1/mod.ts";

export interface DemoAccessUser {
  email: string;
  passwordHash: string;
  createdAt: Date;
}

export async function createDemoAccessUser(email: string, password: string): Promise<DemoAccessUser> {
  const kv = await getKv();
  const salt = await bcrypt.genSalt(8);
  const passwordHash = await bcrypt.hash(password, salt);
  
  const user: DemoAccessUser = {
    email,
    passwordHash,
    createdAt: new Date(),
  };
  
  await kv.set(["demo_access", email], user);
  return user;
}

export async function getDemoAccessUser(email: string): Promise<DemoAccessUser | null> {
  const kv = await getKv();
  const res = await kv.get<DemoAccessUser>(["demo_access", email]);
  return res.value;
}

export async function listDemoAccessUsers(): Promise<DemoAccessUser[]> {
  const kv = await getKv();
  const iter = kv.list<DemoAccessUser>({ prefix: ["demo_access"] });
  const users: DemoAccessUser[] = [];
  for await (const res of iter) {
    users.push(res.value);
  }
  return users;
}

export async function deleteDemoAccessUser(email: string): Promise<void> {
  const kv = await getKv();
  await kv.delete(["demo_access", email]);
}

export async function verifyDemoAccess(email: string, password: string): Promise<boolean> {
  const user = await getDemoAccessUser(email);
  if (!user) return false;
  return await bcrypt.compare(password, user.passwordHash);
}
