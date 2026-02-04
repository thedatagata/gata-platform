/// <reference lib="deno.unstable" />

const KV_PATH = "./db/data.kv";

let kv: Deno.Kv;

export async function getKv(): Promise<Deno.Kv> {
  if (!kv) {
    kv = await Deno.openKv(KV_PATH);
  }
  return kv;
}
