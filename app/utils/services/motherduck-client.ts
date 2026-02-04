import { MDConnection } from "@motherduck/wasm-client";

export async function createMotherDuckClient(token: string) {
  // Config object removed for v0.8.1+ compatibility
  const connection = MDConnection.create({
    mdToken: token,
  });
  
  await connection.isInitialized();
  
  // If you previously relied on "path: 'md.db'" for persistence,
  // that specific configuration pattern is no longer supported in the create() method.
  // You would now typically just rely on the standard MotherDuck cloud storage.
  
  await connection.evaluateQuery("USE my_db;");
  
  console.log("MotherDuck WASM client initialized successfully");
  
  return connection;
}

export async function queryToJSON(client: any, sql: string) {
  const result = await client.evaluateQuery(sql);
  if (result.type === 'materialized') {
    return result.data.toRows();
  }
  throw new Error('Unexpected streaming result');
}

// Streaming query - reads all rows at once (simpler for <100k rows)
export async function streamQueryToJSON(client: any, sql: string) {
  const result = await client.evaluateStreamingQuery(sql);
  await result.dataReader.readAll();
  return result.dataReader.toRows();
}

// Process streaming results with callback
export async function streamQuery(
  client: any, 
  sql: string, 
  onBatch: (rows: any[]) => void | Promise<void>
) {
  const result = await client.streamQuery(sql);
  
  for await (const chunk of result) {
    await onBatch(chunk.toRows());
  }
}

// Stream to response (for API endpoints)
export async function streamToResponse(
  client: any,
  sql: string
): Promise<ReadableStream> {
  const result = await client.streamQuery(sql);
  
  return new ReadableStream({
    async start(controller) {
      for await (const chunk of result) {
        const rows = chunk.toRows();
        controller.enqueue(JSON.stringify(rows) + '\n');
      }
      controller.close();
    }
  });
}

