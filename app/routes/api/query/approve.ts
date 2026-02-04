import { Handlers } from "$fresh/server.ts";
import { getKv } from "../../../utils/system/db.ts";

export const handler: Handlers = {
  // POST: Approve a cached query
  async POST(req) {
    const kv = await getKv();
    try {
      const { id } = await req.json();
      
      if (!id) {
        return new Response(JSON.stringify({ 
          success: false, 
          error: "id parameter required" 
        }), {
          status: 400,
          headers: { "Content-Type": "application/json" },
        });
      }

      const entry = await kv.get(["query_cache", id]);
      if (!entry.value) {
        return new Response(JSON.stringify({ 
          success: false, 
          error: "Query not found" 
        }), {
          status: 404,
          headers: { "Content-Type": "application/json" },
        });
      }

      const cached = entry.value as any;
      cached.approved = true;
      
      await kv.set(["query_cache", id], cached);
      await kv.set(["approved_queries", id], cached);
      
      console.log(`✅ Approved query: ${id}`);

      return new Response(JSON.stringify({ success: true }), {
        headers: { "Content-Type": "application/json" },
      });
    } catch (error) {
      return new Response(JSON.stringify({ 
        success: false, 
        error: (error as Error).message 
      }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  },

  // GET: Get all approved queries
  async GET(req) {
    const kv = await getKv();
    try {
      const url = new URL(req.url);
      const limit = parseInt(url.searchParams.get("limit") || "10");

      const iter = kv.list({ prefix: ["approved_queries"] });
      const queries: any[] = [];

      for await (const entry of iter) {
        queries.push(entry.value);
        if (queries.length >= limit) break;
      }

      return new Response(JSON.stringify({ 
        success: true, 
        queries 
      }), {
        headers: { "Content-Type": "application/json" },
      });
    } catch (error) {
      return new Response(JSON.stringify({ 
        success: false, 
        error: (error as Error).message 
      }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
};
