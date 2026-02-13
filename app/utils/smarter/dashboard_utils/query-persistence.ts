// utils/smarter/dashboard_utils/query-persistence.ts
import type { ChartConfig } from "../autovisualization_dashboard/chart-generator.ts";

export interface PinnedItem {
  id: string;
  tableName: string;
  sql: string;
  prompt: string;
  explanation: string;
  config: ChartConfig;
  timestamp: number;
}

/**
 * SavedQuery is a compatibility type for older components
 */
export type SavedQuery = PinnedItem & {
  table: string;
  dimensions: string[];
  measures: string[];
  resultCount?: number;
};

const INDEXED_DB_NAME = "gata_analytical_persistence";
const INDEXED_DB_VERSION = 2;
const STORE_NAME = "pinned_items";

/**
 * Save a pinned item to IndexedDB
 */
export async function savePinnedItem(item: PinnedItem): Promise<boolean> {
  try {
    await saveToIndexedDB(item);
    return true;
  } catch {
    // Fallback to session storage
    const key = `pins_${item.tableName}`;
    const items = JSON.parse(sessionStorage.getItem(key) || "[]");
    items.push(item);
    sessionStorage.setItem(key, JSON.stringify(items.slice(-10)));
    return false;
  }
}

/**
 * Load all pinned items for a table
 */
export async function loadPinnedItems(tableName: string): Promise<PinnedItem[]> {
  try {
    const all = await loadFromIndexedDB();
    return all.filter(item => item.tableName === tableName);
  } catch {
    return JSON.parse(sessionStorage.getItem(`pins_${tableName}`) || "[]");
  }
}

/**
 * Delete a pinned item
 */
export async function deletePinnedItem(itemId: string, tableName: string): Promise<void> {
  try {
    await deleteFromIndexedDB(itemId);
  } catch {
    const key = `pins_${tableName}`;
    const items = JSON.parse(sessionStorage.getItem(key) || "[]");
    const filtered = items.filter((i: PinnedItem) => i.id !== itemId);
    sessionStorage.setItem(key, JSON.stringify(filtered));
  }
}

/**
 * Compatibility wrapper for loading all queries
 */
export async function loadQueries(): Promise<SavedQuery[]> {
  let items: PinnedItem[] = [];
  try {
    items = await loadFromIndexedDB();
  } catch {
    for (let i = 0; i < sessionStorage.length; i++) {
      const key = sessionStorage.key(i);
      if (key?.startsWith("pins_")) {
        const tableItems = JSON.parse(sessionStorage.getItem(key) || "[]");
        items = [...items, ...tableItems];
      }
    }
  }

  return items.map(item => ({
    ...item,
    table: item.tableName,
    dimensions: item.config.xKey ? [item.config.xKey] : [],
    measures: item.config.yKeys || [],
    resultCount: item.config.data?.length
  }));
}

/**
 * Compatibility wrapper for deleting a query by ID only
 */
export async function deleteQuery(itemId: string): Promise<void> {
  try {
    await deleteFromIndexedDB(itemId);
  } catch {
    for (let i = 0; i < sessionStorage.length; i++) {
      const key = sessionStorage.key(i);
      if (key?.startsWith("pins_")) {
        const items = JSON.parse(sessionStorage.getItem(key) || "[]");
        const filtered = items.filter((item: PinnedItem) => item.id !== itemId);
        sessionStorage.setItem(key, JSON.stringify(filtered));
      }
    }
  }
}

/**
 * Clear all persistence for the current user
 */
export async function clearAllQueries(): Promise<void> {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction([STORE_NAME], "readwrite");
    const store = transaction.objectStore(STORE_NAME);
    const request = store.clear();

    request.onsuccess = () => {
      for (let i = sessionStorage.length - 1; i >= 0; i--) {
        const key = sessionStorage.key(i);
        if (key?.startsWith("pins_")) {
          sessionStorage.removeItem(key);
        }
      }
      resolve();
    };
    request.onerror = () => reject(request.error);
    transaction.oncomplete = () => db.close();
  });
}

// ============ IndexedDB Operations ============

function openDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(INDEXED_DB_NAME, INDEXED_DB_VERSION);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);

    request.onupgradeneeded = (event) => {
      const db = (event.target as IDBOpenDBRequest).result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        const store = db.createObjectStore(STORE_NAME, { keyPath: "id" });
        store.createIndex("tableName", "tableName", { unique: false });
        store.createIndex("timestamp", "timestamp", { unique: false });
      }
    };
  });
}

async function saveToIndexedDB(item: PinnedItem): Promise<void> {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction([STORE_NAME], "readwrite");
    const store = transaction.objectStore(STORE_NAME);
    const request = store.put(item);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error);
    transaction.oncomplete = () => db.close();
  });
}

async function loadFromIndexedDB(): Promise<PinnedItem[]> {
  try {
    const db = await openDB();
    return new Promise((resolve, reject) => {
      const transaction = db.transaction([STORE_NAME], "readonly");
      const store = transaction.objectStore(STORE_NAME);
      const request = store.getAll();
      request.onsuccess = () => {
        const items = request.result as PinnedItem[];
        items.sort((a, b) => b.timestamp - a.timestamp);
        resolve(items);
      };
      request.onerror = () => reject(request.error);
      transaction.oncomplete = () => db.close();
    });
  } catch (error) {
    console.error("IndexedDB load failed", error);
    return [];
  }
}

async function deleteFromIndexedDB(itemId: string): Promise<void> {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction([STORE_NAME], "readwrite");
    const store = transaction.objectStore(STORE_NAME);
    const request = store.delete(itemId);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error);
    transaction.oncomplete = () => db.close();
  });
}
