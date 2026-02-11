import sessionsMetadataJson from "../../../static/smarter/semantic-sessions-metadata.json" with { type: "json" };
import usersMetadataJson from "../../../static/smarter/semantic-users-metadata.json" with { type: "json" };
import { SemanticLayer } from "../../system/semantic-profiler.ts";


// Type definitions for semantic metadata
export interface SemanticMetadata {
  table: string;
  description: string;
  fields: {
    [key: string]: {
      description: string;
      md_data_type: string;
      ingest_data_type: string;
      sanitize: boolean;
      data_type_category: string;
      members: unknown;
    };
  };
  dimensions: {
    [key: string]: {
      alias_name: string;
      transformation: string | null;
      sort?: string;
      members?: string[];
    };
  };
  measures: {
    [key: string]: {
      aggregations?: Record<string, unknown>[];
      formula?: Record<string, unknown>;
      description?: string;
    };
  };
}

// In-memory cache for custom semantic metadata (browser-side)
const customMetadataCache: Record<string, SemanticMetadata> = {};

/**
 * Register custom semantic metadata for a table
 */
export function registerCustomMetadata(metadata: SemanticLayer | SemanticMetadata) {
  // Convert SemanticLayer to SemanticMetadata format
  const mapped: SemanticMetadata = {
    table: metadata.table,
    description: metadata.description,
    fields: metadata.fields,
    dimensions: metadata.dimensions,
    measures: metadata.measures
  };
  customMetadataCache[metadata.table] = mapped;
  console.log(` Registered custom semantic metadata for table: ${metadata.table}`);
}

/**
 * Get semantic metadata for a table
 */
export function getSemanticMetadata(table?: string): SemanticMetadata {
  const selectedTable = table || "sessions";
  if (selectedTable === "sessions") return sessionsMetadataJson as unknown as SemanticMetadata;
  if (selectedTable === "users") return usersMetadataJson as unknown as SemanticMetadata;
  
  // Check custom cache
  if (customMetadataCache[selectedTable]) {
    return customMetadataCache[selectedTable];
  }

  // Fallback to sessions if not found but requested
  return sessionsMetadataJson as unknown as SemanticMetadata;
}

/**
 * Get all table names that have semantic metadata
 */
export function getAllRegisteredTables(): string[] {
  return ["sessions", "users", ...Object.keys(customMetadataCache)];
}



// =============================================================================
// METADATA EXTRACTION FUNCTIONS
// =============================================================================

/**
 * Extract all dimension alias names from metadata
 * These are what users/LLM should use in queries
 */
export function extractAllDimensionNames(table?: string): string[] {
  const metadata = getSemanticMetadata(table);
  return Object.values(metadata.dimensions).map(dim => dim.alias_name);
}

/**
 * Extract all measure alias names from metadata
 * Includes both aggregation aliases and formula keys
 */
export function extractAllMeasureNames(table?: string): string[] {
  const metadata = getSemanticMetadata(table);
  const measureNames: string[] = [];
  
  Object.entries(metadata.measures).forEach(([_baseField, config]) => {
    // Extract aggregation aliases
    if (config.aggregations && Array.isArray(config.aggregations)) {
      config.aggregations.forEach((aggObj) => {
        const aggConfig = Object.values(aggObj)[0] as { alias?: string };
        if (aggConfig.alias) {
          measureNames.push(aggConfig.alias);
        }
      });
    }
    
    // Extract formula keys
    if (config.formula && typeof config.formula === 'object') {
      Object.keys(config.formula).forEach(formulaKey => {
        measureNames.push(formulaKey);
      });
    }
  });
  
  return measureNames;
}

/**
 * Find dimension source column and definition by alias name
 */
export function findDimensionByAlias(aliasName: string, table?: string): {
  sourceColumn: string;
  aliasName: string;
  transformation: string | null;
  description: string | null;
} | null {
  const metadata = getSemanticMetadata(table);
  
  for (const [sourceColumn, dimConfig] of Object.entries(metadata.dimensions)) {
    if (dimConfig.alias_name === aliasName) {
      const field = metadata.fields[sourceColumn];
      return {
        sourceColumn,
        aliasName: dimConfig.alias_name,
        transformation: dimConfig.transformation,
        description: field?.description || null
      };
    }
  }
  
  return null;
}

/**
 * Find measure source column and definition by alias name
 * Returns the aggregation type and base column, or formula SQL
 */
export function findMeasureByAlias(aliasName: string, table?: string): {
  type: 'aggregation' | 'formula';
  sourceColumn: string;
  aggregationType?: string;  // sum, avg, count, etc.
  formulaSQL?: string;
  description: string | null;
  format?: string;
  decimals?: number;
  currency?: string;
} | null {
  const metadata = getSemanticMetadata(table);
  
  for (const [sourceColumn, measureConfig] of Object.entries(metadata.measures)) {
    // Check aggregations
    if (measureConfig.aggregations && Array.isArray(measureConfig.aggregations)) {
      for (const aggObj of measureConfig.aggregations) {
        const aggType = Object.keys(aggObj)[0];
        const aggConfig = aggObj[aggType] as { 
          alias: string; 
          format?: string; 
          decimals?: number; 
          currency?: string 
        };
        
        if (aggConfig.alias === aliasName) {
          return {
            type: 'aggregation',
            sourceColumn,
            aggregationType: aggType,
            description: measureConfig.description || null,
            format: aggConfig.format,
            decimals: aggConfig.decimals,
            currency: aggConfig.currency
          };
        }
      }
    }
    
    // Check formulas
    if (measureConfig.formula && typeof measureConfig.formula === 'object') {
      const formulaConfig = measureConfig.formula[aliasName] as {
        sql: string;
        description?: string;
        format?: string;
        decimals?: number;
        currency?: string;
      } | undefined;
      
      if (formulaConfig) {
        return {
          type: 'formula',
          sourceColumn,
          formulaSQL: formulaConfig.sql,
          description: formulaConfig.description || measureConfig.description || null,
          format: formulaConfig.format,
          decimals: formulaConfig.decimals,
          currency: formulaConfig.currency
        };
      }
    }
  }
  
  return null;
}

/**
 * Get dimension description by alias for display
 */
export function getDimensionDescription(aliasName: string, table?: string): string | null {
  const dim = findDimensionByAlias(aliasName, table);
  return dim?.description || null;
}

/**
 * Get measure description by alias for display
 */
export function getMeasureDescription(aliasName: string, table?: string): string | null {
  const measure = findMeasureByAlias(aliasName, table);
  return measure?.description || null;
}

/**
 * Generate comprehensive catalog for UI display
 * Returns alias names with descriptions and source mapping
 */
export function generateDataCatalog(table?: string): {
  dimensions: Array<{ 
    alias: string; 
    sourceColumn: string;
    description: string;
    hasTransformation: boolean;
  }>;
  measures: Array<{ 
    alias: string; 
    sourceColumn: string;
    description: string; 
    type: 'aggregation' | 'formula';
    aggregationType?: string;
  }>;
  tableName: string;
  tableDescription: string;
} {
  const metadata = getSemanticMetadata(table);
  
  // Build dimensions catalog
  const dimensions = Object.entries(metadata.dimensions).map(([sourceColumn, dimConfig]) => {
    const field = metadata.fields[sourceColumn];
    return {
      alias: dimConfig.alias_name,
      sourceColumn,
      description: field?.description || `Dimension: ${dimConfig.alias_name}`,
      hasTransformation: !!dimConfig.transformation
    };
  });
  
  // Build measures catalog
  const measures: Array<{
    alias: string;
    sourceColumn: string;
    description: string;
    type: 'aggregation' | 'formula';
    aggregationType?: string;
  }> = [];
  
  Object.entries(metadata.measures).forEach(([sourceColumn, measureConfig]) => {
    // Add aggregations
    if (measureConfig.aggregations && Array.isArray(measureConfig.aggregations)) {
      measureConfig.aggregations.forEach((aggObj) => {
        const aggType = Object.keys(aggObj)[0];
        const aggConfig = aggObj[aggType] as { alias?: string };
        
        if (aggConfig.alias) {
          measures.push({
            alias: aggConfig.alias,
            sourceColumn,
            description: measureConfig.description || `${aggType.toUpperCase()} of ${sourceColumn}`,
            type: 'aggregation',
            aggregationType: aggType
          });
        }
      });
    }
    
    // Add formulas
    if (measureConfig.formula && typeof measureConfig.formula === 'object') {
      Object.entries(measureConfig.formula).forEach(([formulaKey, formulaConfig]: [string, any]) => {
        measures.push({
          alias: formulaKey,
          sourceColumn,
          description: formulaConfig.description || measureConfig.description || `Formula: ${formulaKey}`,
          type: 'formula'
        });
      });
    }
  });
  
  return {
    dimensions,
    measures,
    tableName: metadata.table,
    tableDescription: metadata.description
  };
}

// =============================================================================
// SQL GENERATION FUNCTIONS
// =============================================================================

/**
 * Generate comprehensive SQL generation prompt for LLM
 * Shows LLM how to translate alias names into SQL using source columns
 */
export function generateSQLPrompt(table?: string): string {
  const metadata = getSemanticMetadata(table);
  
  // Build dimension translation mappings
  const dimensionMappings: string[] = [];
  Object.entries(metadata.dimensions).forEach(([sourceColumn, dimConfig]) => {
    const alias = dimConfig.alias_name;
    if (dimConfig.transformation) {
      // Has transformation
      dimensionMappings.push(`  "${alias}" → ${dimConfig.transformation}`);
    } else {
      // No transformation - use source column directly
      dimensionMappings.push(`  "${alias}" → ${sourceColumn}`);
    }
  });
  
  // Build measure translation mappings
  const measureMappings: string[] = [];
  Object.entries(metadata.measures).forEach(([sourceColumn, measureConfig]) => {
    // Add aggregations
    if (measureConfig.aggregations && Array.isArray(measureConfig.aggregations)) {
      measureConfig.aggregations.forEach((aggObj) => {
        const aggType = Object.keys(aggObj)[0];
        const aggConfig = aggObj[aggType] as { alias: string };
        const alias = aggConfig.alias;
        
        let sqlPattern = '';
        switch (aggType) {
          case 'sum':
            sqlPattern = `SUM(${sourceColumn})`;
            break;
          case 'avg':
            sqlPattern = `AVG(${sourceColumn})`;
            break;
          case 'count':
            sqlPattern = `COUNT(${sourceColumn})`;
            break;
          case 'count_distinct':
            sqlPattern = `COUNT(DISTINCT ${sourceColumn})`;
            break;
          case 'max':
            sqlPattern = `MAX(${sourceColumn})`;
            break;
          case 'min':
            sqlPattern = `MIN(${sourceColumn})`;
            break;
          default:
            sqlPattern = `${aggType.toUpperCase()}(${sourceColumn})`;
        }
        
        measureMappings.push(`  "${alias}" → ${sqlPattern}`);
      });
    }
    
    // Add formulas
    if (measureConfig.formula && typeof measureConfig.formula === 'object') {
      Object.entries(measureConfig.formula).forEach(([alias, formulaConfig]: [string, any]) => {
        measureMappings.push(`  "${alias}" → ${formulaConfig.sql}`);
      });
    }
  });

  return `You are a specialized SQL Generator for DuckDB. Your task is to translate natural language into a VALID DuckDB SQL query for the "${metadata.table}" table.
 
 ###  TABLE CONTEXT
 Table Name: "${metadata.table}"
 Description: ${metadata.description}
 
 ###  STRICT RULES
 1. ONLY use columns and aliases listed in the MAPPING sections below.
 2. DO NOT hallucinate columns like "opportunity_owner", "total_amount", or "lead_source" unless they are explicitly in the mapping.
 3. All strings must use single quotes: 'direct'.
 4. For boolean columns, use TRUE/FALSE.
 5. For dates, use standard DuckDB syntax: session_date >= '2023-01-01' or session_date >= CURRENT_DATE - INTERVAL '30 days'.
 6. IMPORTANT: If the dataset appears to be historic (older than today), you may use (SELECT MAX(date_column) FROM table) as a base for relative calculations if requested to show "recent" or "last X days".
 7. If the user asks for "sessions" or "count of sessions", ALWAYS use the measure alias "session_count".
 8. If the user asks for "visitors", ALWAYS use the measure alias "unique_visitors".
 9. ALWAYS group by ALL non-aggregated columns if any aggregation is used.
 10. Refer to the table directly as "${metadata.table}".
 
 ###  DIMENSION MAPPING (User Alias → Your SQL Column/Transformation)
 ${dimensionMappings.join('\n')}
 
 ###  MEASURE MAPPING (User Alias → Your SQL Aggregation/Formula)
 ${measureMappings.join('\n')}
 
 ###  EXAMPLES
 - "show me sessions by traffic source": SELECT traffic_source, COUNT(DISTINCT session_id) as session_count FROM ${metadata.table} GROUP BY 1
 - "revenue in the last 7 days": SELECT SUM(session_revenue) as total_revenue FROM ${metadata.table} WHERE session_date >= CURRENT_DATE - INTERVAL '7 days'
 
 RETURN A JSON OBJECT WITH:
 {
   "sql": "the raw SQL query",
   "explanation": "concise 1-sentence explanation"
 }
 
 Generate the SQL for the following request:`;
}

/**
 * Legacy function - kept for backward compatibility
 * Use generateSQLPrompt instead for SQL generation
 */
export function generatePivotWebLLMPrompt(table?: string): string {
  return generateSQLPrompt(table);
}

/**
 * Sanitize a single value (convert Uint8Array and BigInt to Number)
 */
export function sanitizeValue(value: unknown): unknown {
  if (typeof value === 'bigint') {
    return Number(value);
  } else if (value instanceof Uint8Array) {
    let result = 0;
    for (let i = 0; i < Math.min(8, value.length); i++) {
      result += value[i] * Math.pow(256, i);
    }
    return result;
  } else if (value instanceof Date) {
    return value.toISOString().split('T')[0];
  }
  return value;
}

/**
 * Sanitize an entire row of data
 */
export function sanitizeRow(row: Record<string, unknown>, _metadata: SemanticMetadata): Record<string, unknown> {
  const sanitized: Record<string, unknown> = {};
  
  for (const [key, value] of Object.entries(row)) {
    sanitized[key] = sanitizeValue(value);
  }
  
  return sanitized;
}
