// utils/services/metadata-store.ts
import type { ColumnMetadata } from "./table-profiler.ts";

const DB_NAME = "dasgata_metadata";
const DB_VERSION = 2;
const STORE_NAME = "table_profiles";
const MAX_CACHED_TABLES = 5; // ← Cache limit

interface TableProfile {
  table_name: string;
  columns: ColumnMetadata[];
  createdAt: number;
}

export interface CacheStatus {
  count: number;
  limit: number;
  isAtLimit: boolean;
  tables: Array<{ name: string; createdAt: number }>;
}

class MetadataStore {
  private db: IDBDatabase | null = null;

  async init(): Promise<void> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);

      request.onerror = () => reject(request.error);
      request.onsuccess = () => {
        this.db = request.result;
        resolve();
      };

      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;
        
        if (db.objectStoreNames.contains(STORE_NAME)) {
          db.deleteObjectStore(STORE_NAME);
        }
        
        const store = db.createObjectStore(STORE_NAME, { keyPath: "table_name" });
        store.createIndex("table_name", "table_name", { unique: true });
        store.createIndex("createdAt", "createdAt", { unique: false });
      };
    });
  }

  /**
   * Get current cache status
   */
  async getCacheStatus(): Promise<CacheStatus> {
    const profiles = await this.getAllTableMetadata();
    
    return {
      count: profiles.length,
      limit: MAX_CACHED_TABLES,
      isAtLimit: profiles.length >= MAX_CACHED_TABLES,
      tables: profiles
        .map(p => ({ name: p.table_name, createdAt: p.createdAt }))
        .sort((a, b) => b.createdAt - a.createdAt) // newest first
    };
  }

  /**
   * Check if cache is at limit
   */
  async isAtCacheLimit(): Promise<boolean> {
    const profiles = await this.getAllTableMetadata();
    return profiles.length >= MAX_CACHED_TABLES;
  }

  /**
   * Delete oldest cached table
   */
  async deleteOldestTable(): Promise<string | null> {
    const profiles = await this.getAllTableMetadata();
    
    if (profiles.length === 0) return null;
    
    // Find oldest table
    const oldest = profiles.reduce((prev, current) => 
      current.createdAt < prev.createdAt ? current : prev
    );
    
    await this.deleteTableMetadata(oldest.table_name);
    return oldest.table_name;
  }

  /**
   * Auto-cleanup: delete oldest if at limit
   */
  async autoCleanupIfNeeded(): Promise<string | null> {
    const isAtLimit = await this.isAtCacheLimit();
    
    if (isAtLimit) {
      console.log('⚠️ Cache limit reached, auto-deleting oldest table');
      return await this.deleteOldestTable();
    }
    
    return null;
  }

  async saveTableMetadata(tableName: string, columns: ColumnMetadata[]): Promise<void> {
    if (!this.db) await this.init();
    
    if (!tableName) {
      throw new Error("Table name is required");
    }

    if (columns.length === 0) {
      throw new Error("Cannot save empty column metadata");
    }

    // Check if table already exists (updates don't count against limit)
    const existing = await this.getTableMetadata(tableName);
    
    // If new table and at limit, throw error
    if (!existing) {
      const isAtLimit = await this.isAtCacheLimit();
      if (isAtLimit) {
        const status = await this.getCacheStatus();
        throw new Error(
          `CACHE_LIMIT_EXCEEDED: Maximum ${MAX_CACHED_TABLES} tables cached. ` +
          `Please clear some tables before caching new ones. ` +
          `Current tables: ${status.tables.map(t => t.name).join(', ')}`
        );
      }
    }

    const profile: TableProfile = {
      table_name: tableName,
      columns: columns,
      createdAt: Math.floor(Date.now() / 1000)
    };
    
    return new Promise((resolve, reject) => {
      const transaction = this.db!.transaction([STORE_NAME], "readwrite");
      const store = transaction.objectStore(STORE_NAME);
      const request = store.put(profile);

      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }

  async getTableMetadata(table_name: string): Promise<ColumnMetadata[] | null> {
    if (!this.db) await this.init();

    return new Promise((resolve, reject) => {
      const transaction = this.db!.transaction([STORE_NAME], "readonly");
      const store = transaction.objectStore(STORE_NAME);
      const request = store.get(table_name);

      request.onsuccess = () => {
        const profile = request.result as TableProfile | undefined;
        resolve(profile ? profile.columns : null);
      };
      request.onerror = () => reject(request.error);
    });
  }

  async getAllTableMetadata(): Promise<TableProfile[]> {
    if (!this.db) await this.init();

    return new Promise((resolve, reject) => {
      const transaction = this.db!.transaction([STORE_NAME], "readonly");
      const store = transaction.objectStore(STORE_NAME);
      const request = store.getAll();

      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  async deleteTableMetadata(table_name: string): Promise<void> {
    if (!this.db) await this.init();

    return new Promise((resolve, reject) => {
      const transaction = this.db!.transaction([STORE_NAME], "readwrite");
      const store = transaction.objectStore(STORE_NAME);
      const request = store.delete(table_name);

      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }

  async clearAll(): Promise<void> {
    if (!this.db) await this.init();

    return new Promise((resolve, reject) => {
      const transaction = this.db!.transaction([STORE_NAME], "readwrite");
      const store = transaction.objectStore(STORE_NAME);
      const request = store.clear();

      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }
}

export const metadataStore = new MetadataStore();
