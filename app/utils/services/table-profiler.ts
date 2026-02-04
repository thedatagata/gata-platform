// utils/services/table-profiler.ts

export interface ColumnMetadata {
  table_name: string;
  column_name: string;
  type: string;
  minValue?: string | number;
  maxValue?: string | number;
  meanValue?: number;
  uniqueCount?: number;
  nullPercentage?: number;
  topValues?: Array<{ value: any; count: number }>;
  isNumeric: boolean;
  isDate: boolean;
  isBoolean: boolean;
  createdAt?: number;
}

export class TableProfiler {
  /**
   * Check if table has been profiled
   */
  static async needsProfiling(table: string): Promise<boolean> {
    const { metadataStore } = await import('./metadata-store.ts');
    const existing = await metadataStore.getTableMetadata(table);
    return !existing;
  }

  /**
   * Profile a table and return column metadata
   */
  static async profileTable(
    client: any,
    tableName: string,
    sampleSize?: number
  ): Promise<ColumnMetadata[]> {
    return await this.profileColumns(client, tableName);
  }

  /**
   * Profile all columns in a table
   */
  private static async profileColumns(
    client: any,
    table: string
  ): Promise<ColumnMetadata[]> {
    // Use SUMMARIZE for quick stats
    const summaryResult = await client.query(`
      SUMMARIZE SELECT * FROM ${table}
    `);
    const summaryRows = summaryResult.toArray().map(row => row.toJSON());

    const columns: ColumnMetadata[] = [];

    for (const row of summaryRows) {
      const isNumeric = ['INTEGER', 'BIGINT', 'DOUBLE', 'DECIMAL', 'FLOAT'].some(
        t => row.column_type?.toUpperCase().includes(t)
      );
      const isDate = row.column_type?.toUpperCase().includes('DATE') || 
                     row.column_type?.toUpperCase().includes('TIMESTAMP');
      const isBoolean = row.column_type?.toUpperCase().includes('BOOLEAN');

      const col: ColumnMetadata = {
        table_name: table,
        column_name: row.column_name,
        type: row.column_type,
        isNumeric,
        isDate,
        isBoolean,
        uniqueCount: row.approx_unique,
        nullPercentage: parseFloat(row.null_percentage) || 0,
        createdAt: Math.floor(Date.now() / 1000)
      };

      // Add numeric stats
      if (isNumeric && row.min !== null && row.max !== null) {
        col.minValue = parseFloat(row.min);
        col.maxValue = parseFloat(row.max);
        col.meanValue = parseFloat(row.avg);
      }

      // For low-cardinality columns, get top values
      if (row.approx_unique && row.approx_unique <= 20) {
        try {
          const topValuesResult = await client.query(`
            SELECT ${row.column_name} as value, COUNT(*) as count
            FROM ${table}
            WHERE ${row.column_name} IS NOT NULL
            GROUP BY ${row.column_name}
            ORDER BY count DESC
            LIMIT 10
          `);
          col.topValues = topValuesResult.toArray().map(row => row.toJSON());
        } catch (err) {
          console.warn(`Failed to get top values for ${row.column_name}:`, err);
        }
      }

      columns.push(col);
    }
    return columns;
  }
}