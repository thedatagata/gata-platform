import { Handlers } from "$fresh/server.ts";
import { createMotherDuckClient, queryToJSON } from "../../../../utils/services/motherduck-client.ts";

export const handler: Handlers = {
  async GET(_req, ctx) {
    const { tenant } = ctx.params;
    const token = Deno.env.get("MOTHERDUCK_TOKEN");

    if (!token) {
      return new Response(JSON.stringify({ error: "Server misconfigured: MOTHERDUCK_TOKEN missing" }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }

    try {
      const client = await createMotherDuckClient(token);
      
      // Query the view for this tenant
      const sql = `
        SELECT 
          is_semantic_layer_ready, 
          dlt_load_id, 
          last_dbt_status
        FROM my_db.main.obs_platform_status__semantic_readiness
        WHERE tenant_slug = '${tenant}'
      `;
      
      // We use queryToJSON to get rows
      const rows = await queryToJSON(client, sql);
      
      // Logic: Ready if we have rows and ALL are ready.
      // If no rows, then not ready (maybe not even started).
      
      if (!rows || rows.length === 0) {
         return new Response(JSON.stringify({ 
           is_ready: false, 
           last_load_id: null, 
           status: "starting",
           message: "Pipeline initializing..."
         }), {
           headers: { "Content-Type": "application/json" }
         });
      }

      const allReady = rows.every((r: any) => r.is_semantic_layer_ready === true);
      const anyError = rows.some((r: any) => r.last_dbt_status === 'error');
      const loadId = rows[0]?.dlt_load_id || null;

      let status = "processing";
      if (anyError) status = "error";
      else if (allReady) status = "ready";
      else status = "modeling"; // Some exist but not all ready

      return new Response(JSON.stringify({
        is_ready: allReady,
        last_load_id: loadId,
        status: status,
        message: allReady ? "Ready" : (anyError ? "Pipeline Error" : "Processing...")
      }), {
        headers: { "Content-Type": "application/json" }
      });

    } catch (err) {
      console.error(`Readiness check failed for ${tenant}:`, err);
      return new Response(JSON.stringify({ 
        is_ready: false, 
        last_load_id: null, 
        status: "error",
        message: (err as Error).message 
      }), {
        status: 500,
        headers: { "Content-Type": "application/json" }
      });
    }
  }
};
