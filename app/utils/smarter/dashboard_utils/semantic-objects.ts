// utils/smarter/dashboard_utils/semantic-objects.ts
import { 
  getSemanticMetadata, 
  findDimensionByAlias, 
  findMeasureByAlias,
  sanitizeRow,
  type SemanticMetadata
} from "./semantic-config.ts";

interface QuerySpec {
  dimensions?: string[];  // Array of dimension ALIASES
  measures?: string[];    // Array of measure ALIASES
  filters?: string[];
  orderBy?: (string | number)[];     // Array of columns to order by
  limit?: number;
}

export class SemanticReportObj {
  private metadata: SemanticMetadata;
  private table: string;

  constructor(public db: unknown, table: string) {
    this.table = table;
    this.metadata = getSemanticMetadata(table);
  }

  /**
   * Robust casting for temporal fields that might be strings
   */
  private getTemporalCastSQL(colRef: string): string {
    const strcol = `CAST(${colRef} AS VARCHAR)`;
    return `COALESCE(
      TRY_CAST(${colRef} AS TIMESTAMP),
      TRY_CAST(${colRef} AS DATE),
      try_strptime(${strcol}, '%m/%d/%Y'),
      try_strptime(${strcol}, '%d/%m/%Y'),
      try_strptime(${strcol}, '%Y-%m-%d'),
      try_strptime(${strcol}, '%Y/%m/%d'),
      try_strptime(${strcol}, '%m-%d-%Y')
    )`;
  }

  /**
   * Execute a semantic query using alias names
   */
  async query(opts: QuerySpec) {
    if (!opts.dimensions && !opts.measures) {
      throw new Error("Query must specify at least one dimension or measure");
    }

    const selectCols = [
      ...(opts.dimensions?.map(alias => this.getDimensionSQL(alias)) || []),
      ...(opts.measures?.map(alias => this.getMeasureSQL(alias)) || [])
    ];

    // Build filters
    const allFilters: string[] = [];
    if (opts.filters) {
      allFilters.push(...opts.filters);
    }

    const sql = `
      SELECT ${selectCols.join(", ")}
      FROM "${this.metadata.table}"
      ${allFilters.length ? `WHERE ${allFilters.join(" AND ")}` : ""}
      ${opts.dimensions?.length ? `GROUP BY ${opts.dimensions.map((_d, i) => i + 1).join(", ")}` : ""}
      ${opts.orderBy?.length ? `ORDER BY ${opts.orderBy.join(", ")}` : ""}
      ${opts.limit ? `LIMIT ${opts.limit}` : ""}
    `.trim();

    console.log("🔍 [SemanticTable] Generated SQL:", sql);

    const database = (this.db as { query: (sql: string) => Promise<{ toArray: () => Record<string, unknown>[] }> });
    const result = await database.query(sql);
    const rows = result.toArray().map((row: Record<string, unknown>) => typeof row.toJSON === 'function' ? (row as any).toJSON() : row);
    
    // Sanitize data (handle Uint8Array and BigInt)
    const sanitized = rows.map((row: Record<string, unknown>) => sanitizeRow(row, this.metadata));
    
    console.log(`✅ [SemanticTable] Query returned ${sanitized.length} rows`);
    return sanitized;
  }

  /**
   * Generate dimension SQL from alias name
   * Looks up the source column and transformation
   */
  private getDimensionSQL(aliasName: string): string {
    const dimDef = findDimensionByAlias(aliasName, this.table);
    
    if (!dimDef) {
      // Try looking up in the metadata directly if it's a custom table
      const customDim = Object.entries(this.metadata.dimensions).find(([_k, d]) => d.alias_name === aliasName);
      if (customDim) {
         const [sourceColumn, dimConfig] = customDim;
         if (dimConfig.transformation) return `(${dimConfig.transformation}) as "${aliasName}"`;
         return `"${this.metadata.table}"."${sourceColumn}" as "${aliasName}"`;
      }

      // FALLBACK: If aliasName is actually a source column in metadata.fields/dimensions
      if (this.metadata.fields[aliasName] || this.metadata.dimensions[aliasName]) {
         const dimConfig = this.metadata.dimensions[aliasName];
         const field = this.metadata.fields[aliasName];
         
         if (dimConfig?.transformation) return `(${dimConfig.transformation}) as "${aliasName}"`;
         
         // Auto-cast temporal strings
         if (field?.data_type_category === 'temporal' && field?.ingest_data_type === 'string') {
           const colRef = `"${this.metadata.table}"."${aliasName}"`;
           return `${this.getTemporalCastSQL(colRef)} as "${aliasName}"`;
         }

         return `"${this.metadata.table}"."${aliasName}" as "${aliasName}"`;
      }

      throw new Error(`Unknown dimension alias: ${aliasName}. Check data catalog for available dimensions.`);
    }
    
    // If it's a transformation, use it
    if (dimDef.transformation) {
      return `(${dimDef.transformation}) as "${aliasName}"`;
    }
    
    // Auto-cast temporal strings
    const field = this.metadata.fields[dimDef.sourceColumn];
    if (field?.data_type_category === 'temporal' && field?.ingest_data_type === 'string') {
      const colRef = `"${this.metadata.table}"."${dimDef.sourceColumn}"`;
      return `${this.getTemporalCastSQL(colRef)} as "${aliasName}"`;
    }

    // Otherwise use the source column directly
    return `"${this.metadata.table}"."${dimDef.sourceColumn}" as "${aliasName}"`;
  }

  /**
   * Generate measure SQL from alias name
   * Looks up whether it's an aggregation or formula
   */
  private getMeasureSQL(aliasName: string): string {
    const measureDef = findMeasureByAlias(aliasName, this.table);
    
    if (!measureDef) {
       // Manual lookup for custom measures
       for (const [sourceColumn, measureConfig] of Object.entries(this.metadata.measures)) {
          if (measureConfig.aggregations) {
             for (const aggObj of measureConfig.aggregations) {
                const type = Object.keys(aggObj)[0];
                const config = (aggObj as Record<string, any>)[type];
                if (config.alias === aliasName) {
                   // Add TRY_CAST safety for numerical aggregations if source might be string
                   const isNumericAgg = ['sum', 'avg', 'max', 'min'].includes(type.toLowerCase());
                   const colRef = isNumericAgg ? `TRY_CAST("${this.metadata.table}"."${sourceColumn}" AS DOUBLE)` : `"${this.metadata.table}"."${sourceColumn}"`;
                   return `${type.toUpperCase()}(${colRef}) as "${aliasName}"`;
                }
             }
          }
          if (measureConfig.formula && (measureConfig.formula as any)[aliasName]) {
             return `(${(measureConfig.formula as any)[aliasName].sql}) as "${aliasName}"`;
          }
       }

       // FALLBACK: If it's a raw column with a basic aggregation request or just raw
       if (this.metadata.measures[aliasName]) {
          const m = this.metadata.measures[aliasName];
          if (m.aggregations && m.aggregations.length > 0) {
              const aggObj = m.aggregations[0];
              const type = Object.keys(aggObj)[0];
              const config = (aggObj as Record<string, any>)[type];
              return `${type.toUpperCase()}("${this.metadata.table}"."${aliasName}") as "${config.alias || aliasName}"`;
          }
       }

       throw new Error(`Unknown measure alias: ${aliasName}. Check data catalog for available measures.`);
    }
    
    if (measureDef.type === 'aggregation') {
      const isNumericAgg = ['sum', 'avg', 'max', 'min'].includes(measureDef.aggregationType?.toLowerCase() || '');
      const columnRef = measureDef.sourceColumn === '_count' 
        ? '*' 
        : (isNumericAgg 
            ? `TRY_CAST("${this.metadata.table}"."${measureDef.sourceColumn}" AS DOUBLE)` 
            : `"${this.metadata.table}"."${measureDef.sourceColumn}"`);
      
      let sql: string;
      const aggType = measureDef.aggregationType || 'count';
      
      switch (aggType) {
        case "sum": sql = `SUM(${columnRef})`; break;
        case "avg": sql = `AVG(${columnRef})`; break;
        case "count": sql = `COUNT(${columnRef})`; break;
        case "count_distinct": sql = `COUNT(DISTINCT ${columnRef})`; break;
        case "max": sql = `MAX(${columnRef})`; break;
        case "min": sql = `MIN(${columnRef})`; break;
        default: sql = `${aggType.toUpperCase()}(${columnRef})`;
      }
      
      return `${sql} as "${aliasName}"`;
    } else {
      return `(${measureDef.formulaSQL}) as "${aliasName}"`;
    }
  }

  /**
   * Generate SQL for preview/debugging
   */
  generateSQL(opts: QuerySpec): string {
    const selectCols = [
      ...(opts.dimensions?.map(alias => this.getDimensionSQL(alias)) || []),
      ...(opts.measures?.map(alias => this.getMeasureSQL(alias)) || [])
    ];

    const allFilters: string[] = [];
    if (opts.filters) {
      allFilters.push(...opts.filters);
    }

    return `
      SELECT ${selectCols.join(", ")}
      FROM ${this.metadata.table}
      ${allFilters.length ? `WHERE ${allFilters.join(" AND ")}` : ""}
      ${opts.dimensions?.length ? `GROUP BY ${opts.dimensions.map((_d, i) => i + 1).join(", ")}` : ""}
      ${opts.orderBy?.length ? `ORDER BY ${opts.orderBy.join(", ")}` : ""}
      ${opts.limit ? `LIMIT ${opts.limit}` : ""}
    `.trim();
  }

  /**
   * Get metadata for UI/debugging
   */
  getMetadata() {
    return {
      table: this.metadata.table,
      description: this.metadata.description,
      dimensions: this.metadata.dimensions,
      measures: this.metadata.measures,
      fields: this.metadata.fields
    };
  }
}


/**
 * Factory function to create semantic table
 */
export function createSemanticReportObj(db: unknown, table: string): SemanticReportObj {
  return new SemanticReportObj(db, table);
}

/**
 * Create semantic tables for both sessions and users
 */
import { getAllRegisteredTables } from "./semantic-config.ts";

export function createSemanticTables(db: unknown) {
  const tables: Record<string, SemanticReportObj> = {};
  
  // Get all registered tables including sessions, users, and custom ones
  const registeredTables = getAllRegisteredTables();
  registeredTables.forEach(tableName => {
    tables[tableName] = new SemanticReportObj(db, tableName);
  });

  return tables;
}

