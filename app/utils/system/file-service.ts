/**
 * Utility for handling file uploads to DuckDB
 */

// deno-lint-ignore no-explicit-any
export async function processFileUpload(
  db: any, 
  file: File, 
  options: { useLocalBuffer?: boolean } = {}
): Promise<string> {

  const fileName = file.name;
  const tableName = fileName.split('.')[0].replace(/[^a-zA-Z0-9_]/g, '_'); // Sanitize

  try {
    // 1. Register the file in DuckDB's virtual filesystem
    // Support for both MotherDuck MDConnection and standard DuckDBConnection
    // We try multiple ways to find the underlying DuckDB instance
    const duckdb = (db as any)?.db || 
                   (db as any)?.instance || 
                   (db as any)?.duckdb || 
                   (db as any)?.getDatabase?.() ||
                   db;
    
    // Guard the rollout of the buffer-based approach
    const useLocalBuffer = options.useLocalBuffer ?? false;
    console.log(` [FileService] Registration check: options.useLocalBuffer=${options.useLocalBuffer}, final=${useLocalBuffer}`);

    if (useLocalBuffer) {
        console.log(" [FileService] Using local buffer approach (guarded rollout)");
        // Convert file to Uint8Array for registerFileBuffer (more compatible than registerFileHandle)
        const arrayBuffer = await file.arrayBuffer();
        const uint8Array = new Uint8Array(arrayBuffer);

        // Check for registerFileBuffer existence
        const hasBufferMethod = (duckdb && typeof (duckdb as any).registerFileBuffer === 'function') || 
                               (db && typeof (db as any).registerFileBuffer === 'function');
        
        if (hasBufferMethod) {
            if (duckdb && typeof (duckdb as any).registerFileBuffer === 'function') {
                await (duckdb as any).registerFileBuffer(fileName, uint8Array);
            } else {
                await (db as any).registerFileBuffer(fileName, uint8Array);
            }
        } else {
            console.warn(" [FileService] registerFileBuffer not supported, falling back to registerFileHandle");
            await registerViaHandle(duckdb, db, fileName, file);
        }
    } else {
        console.log(" [FileService] Falling back to standard file handle approach (Flag is OFF)");
        await registerViaHandle(duckdb, db, fileName, file);
    }


    // 2. Create a table from the file (Auto-detects Parquet, CSV, or JSON)
    // MotherDuck MDConnection uses evaluateQuery, DuckDBConnection uses query
    const queryMethod = (db as any)?.evaluateQuery ? 'evaluateQuery' : 'query';
    
    const dbObj = db as any;
    if (dbObj && typeof dbObj[queryMethod] !== 'function') {
      throw new Error(`Database connection method '${queryMethod}' not found`);
    }

    await dbObj[queryMethod](`CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM '${fileName}'`);

    console.log(` Successfully loaded ${fileName} into table ${tableName}`);
    return tableName;
  } catch (err) {
    console.error("File upload utility failed:", err);
    throw err;
  }
}

// deno-lint-ignore no-explicit-any
async function registerViaHandle(duckdb: any, db: any, fileName: string, file: File) {
    if (duckdb && typeof (duckdb as any).registerFileHandle === 'function') {
        // 4 = DuckDBDataProtocol.BROWSER_FILEREADER
        await (duckdb as any).registerFileHandle(fileName, file, 4, true);
    } else if (db && typeof (db as any).registerFileHandle === 'function') {
        await (db as any).registerFileHandle(fileName, file, 4, true);
    } else {
        throw new Error("DuckDB client does not support registerFileHandle");
    }
}

