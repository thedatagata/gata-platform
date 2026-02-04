// utils/services/table-config.ts
export interface TableConfig {
  fullName: string;
  mode: "stream" | "materialize";
  configuredAt: string;
  source: "browser" | "motherduck";
}

const CONFIG_KEY = "configured_tables";

export class TableConfigManager {
  /**
   * Get all configured tables
   */
  static getConfiguredTables(): TableConfig[] {
    try {
      const stored = localStorage.getItem(CONFIG_KEY);
      return stored ? JSON.parse(stored) : [];
    } catch (error) {
      console.error("Failed to load table configuration:", error);
      return [];
    }
  }

  /**
   * Add or update a table configuration
   */
  static configureTable(
    fullName: string,
    mode: "stream" | "materialize",
    source: "browser" | "motherduck",
  ): void {
    const configs = this.getConfiguredTables();
    const existing = configs.findIndex((c) => c.fullName === fullName);
    
    const config: TableConfig = {
      fullName,
      mode,
      source,
      configuredAt: new Date().toISOString(),
    };

    if (existing >= 0) {
      configs[existing] = config;
    } else {
      configs.push(config);
    }

    localStorage.setItem(CONFIG_KEY, JSON.stringify(configs));
    console.log(`✅ Configured table: ${fullName} (${mode})`);
  }

  /**
   * Remove a table configuration
   */
  static unconfigureTable(fullName: string): void {
    const configs = this.getConfiguredTables();
    const filtered = configs.filter((c) => c.fullName !== fullName);
    localStorage.setItem(CONFIG_KEY, JSON.stringify(filtered));
    console.log(`❌ Removed configuration: ${fullName}`);
  }

  /**
   * Check if a table is configured
   */
  static isConfigured(fullName: string): boolean {
    return this.getConfiguredTables().some((c) => c.fullName === fullName);
  }

  /**
   * Get configuration for a specific table
   */
  static getTableConfig(fullName: string): TableConfig | null {
    return this.getConfiguredTables().find((c) => c.fullName === fullName) || null;
  }

  /**
   * Get only materialized tables (in browser memory)
   */
  static getMaterializedTables(): TableConfig[] {
    return this.getConfiguredTables().filter((c) => c.mode === "materialize");
  }

  /**
   * Get only streaming tables (remote)
   */
  static getStreamingTables(): TableConfig[] {
    return this.getConfiguredTables().filter((c) => c.mode === "stream");
  }

  /**
   * Clear all configurations
   */
  static clearAll(): void {
    localStorage.removeItem(CONFIG_KEY);
    console.log("🗑️ Cleared all table configurations");
  }
}
