// utils/smarter/autovisualization_dashboard/semantic-catalog-generator.ts
import { getSemanticMetadata } from "../dashboard_utils/semantic-config.ts";

/**
 * Dimension catalog entry with alias and description
 */
export interface DimensionCatalogEntry {
  alias: string;
  column: string;
  description: string;
  type: "temporal" | "categorical" | "boolean" | "sequential";
  transformation?: string;
  sort?: string;
}

/**
 * Measure catalog entry with alias, aggregation type, and description
 */
export interface MeasureCatalogEntry {
  alias: string;
  baseField: string;
  description: string;
  aggregationType?: "sum" | "avg" | "count" | "count_distinct" | "max" | "min";
  formula?: string;
  format: string;
  decimals?: number;
}

/**
 * Complete semantic catalog with all available fields
 */
export interface SemanticCatalog {
  table: string;
  tableDescription: string;
  dimensions: DimensionCatalogEntry[];
  measures: MeasureCatalogEntry[];
}

/**
 * Generate comprehensive semantic catalog by parsing metadata structure
 * Extracts all dimension aliases and measure aliases (from aggregations + formulas)
 */
export function generateSemanticCatalog(table?: "sessions" | "users"): SemanticCatalog {
  const metadata = getSemanticMetadata(table);
  
  const dimensions: DimensionCatalogEntry[] = [];
  const measures: MeasureCatalogEntry[] = [];
  
  // Parse dimensions
  Object.entries(metadata.dimensions).forEach(([dimKey, dimConfig]) => {
    const field = metadata.fields[dimKey];
    const alias = dimConfig.alias_name || dimKey;
    
    // Determine dimension type
    let type: DimensionCatalogEntry["type"] = "categorical";
    if (dimKey.includes('date') || dimKey.includes('time') || dimKey.includes('month') || dimKey.includes('weekday')) {
      type = "temporal";
    } else if (dimConfig.sort?.startsWith("custom:")) {
      type = "sequential";
    } else if (dimKey.startsWith('is_') || dimKey.startsWith('has_') || field?.md_data_type === 'BOOLEAN') {
      type = "boolean";
    }
    
    dimensions.push({
      alias,
      column: dimKey,
      description: field?.description || `${dimKey} dimension`,
      type,
      transformation: dimConfig.transformation || undefined,
      sort: dimConfig.sort || undefined
    });
  });
  
  // Parse measures - extract ALL aggregation aliases and formula keys
  Object.entries(metadata.measures).forEach(([baseField, measureConfig]) => {
    // Extract aggregation aliases
    if (measureConfig.aggregations && Array.isArray(measureConfig.aggregations)) {
      measureConfig.aggregations.forEach((aggObj: any) => {
        const aggType = Object.keys(aggObj)[0] as MeasureCatalogEntry["aggregationType"];
        const aggConfig = aggObj[aggType];
        
        if (aggConfig.alias) {
          measures.push({
            alias: aggConfig.alias,
            baseField,
            description: `${aggType.toUpperCase()} of ${baseField}`,
            aggregationType: aggType,
            format: aggConfig.format || "number",
            decimals: aggConfig.decimals
          });
        }
      });
    }
    
    // Extract formula keys
    if (measureConfig.formula && typeof measureConfig.formula === 'object') {
      Object.entries(measureConfig.formula).forEach(([formulaKey, formulaConfig]: [string, any]) => {
        measures.push({
          alias: formulaKey,
          baseField,
          description: formulaConfig.description || `${formulaKey} (formula)`,
          formula: formulaConfig.sql,
          format: formulaConfig.format || "number",
          decimals: formulaConfig.decimals
        });
      });
    }
  });
  
  return {
    table: metadata.table,
    tableDescription: metadata.description,
    dimensions,
    measures
  };
}

/**
 * Generate LLM prompt with comprehensive field catalog
 */
export function generateLLMPromptWithCatalog(table?: "sessions" | "users"): string {
  const catalog = generateSemanticCatalog(table);
  
  // Group dimensions by type
  const temporalDims = catalog.dimensions.filter(d => d.type === "temporal");
  const categoricalDims = catalog.dimensions.filter(d => d.type === "categorical");
  const booleanDims = catalog.dimensions.filter(d => d.type === "boolean");
  const sequentialDims = catalog.dimensions.filter(d => d.type === "sequential");
  
  // Group measures by pattern
  const eventMeasures = catalog.measures.filter(m => m.alias.includes('event'));
  const rateMeasures = catalog.measures.filter(m => m.alias.includes('rate') || m.alias.includes('percentage'));
  const revenueMeasures = catalog.measures.filter(m => m.alias.includes('revenue'));
  const countMeasures = catalog.measures.filter(m => 
    m.alias.includes('count') || m.alias.includes('session') || m.alias.includes('visitor') || m.alias.includes('user')
  );
  const avgMeasures = catalog.measures.filter(m => m.alias.startsWith('avg_'));
  const totalMeasures = catalog.measures.filter(m => m.alias.startsWith('total_'));
  const otherMeasures = catalog.measures.filter(m => 
    !eventMeasures.includes(m) && !rateMeasures.includes(m) && 
    !revenueMeasures.includes(m) && !countMeasures.includes(m) &&
    !avgMeasures.includes(m) && !totalMeasures.includes(m)
  );

  return `You are a SQL generator for the ${catalog.table} table.
${catalog.tableDescription}

CRITICAL: Output ONLY raw SQL. No markdown, no code blocks, no explanations.

═══════════════════════════════════════════════════════════════
AVAILABLE DIMENSIONS (for GROUP BY)
═══════════════════════════════════════════════════════════════

TEMPORAL DIMENSIONS:
${temporalDims.map(d => `  • ${d.alias} - ${d.description}`).join('\n') || '  (none)'}

CATEGORICAL DIMENSIONS:
${categoricalDims.map(d => `  • ${d.alias} - ${d.description}`).join('\n') || '  (none)'}

BOOLEAN/FLAG DIMENSIONS:
${booleanDims.map(d => `  • ${d.alias} - ${d.description}`).join('\n') || '  (none)'}

SEQUENTIAL DIMENSIONS (ordered):
${sequentialDims.map(d => `  • ${d.alias} - ${d.description}`).join('\n') || '  (none)'}

═══════════════════════════════════════════════════════════════
AVAILABLE MEASURES (for SELECT - pre-aggregated)
═══════════════════════════════════════════════════════════════

EVENT MEASURES:
${eventMeasures.map(m => `  • ${m.alias} - ${m.description}`).join('\n') || '  (none)'}

RATE/PERCENTAGE MEASURES:
${rateMeasures.map(m => `  • ${m.alias} - ${m.description}`).join('\n') || '  (none)'}

REVENUE MEASURES:
${revenueMeasures.map(m => `  • ${m.alias} - ${m.description}`).join('\n') || '  (none)'}

COUNT MEASURES:
${countMeasures.map(m => `  • ${m.alias} - ${m.description}`).join('\n') || '  (none)'}

AVERAGE MEASURES:
${avgMeasures.map(m => `  • ${m.alias} - ${m.description}`).join('\n') || '  (none)'}

TOTAL MEASURES:
${totalMeasures.map(m => `  • ${m.alias} - ${m.description}`).join('\n') || '  (none)'}

OTHER MEASURES:
${otherMeasures.map(m => `  • ${m.alias} - ${m.description}`).join('\n') || '  (none)'}

═══════════════════════════════════════════════════════════════
SQL GENERATION RULES
═══════════════════════════════════════════════════════════════

1. Table name: ${catalog.table}
2. Use ONLY the aliases listed above - these are the exact field names
3. Measures are PRE-AGGREGATED - do NOT wrap in SUM(), AVG(), COUNT(), etc.
4. For dimensions with transformations, use the alias directly (handled automatically)
5. Format: SELECT <measures>, <dimensions> FROM ${catalog.table} GROUP BY <dimensions>

EXAMPLES:

Request: "average interest events by traffic source type"
SQL: SELECT traffic_source_type, avg_interest_events FROM ${catalog.table} GROUP BY traffic_source_type

Request: "total revenue and session count by session date"
SQL: SELECT session_date, total_revenue, session_count FROM ${catalog.table} GROUP BY session_date

Request: "trial signup rate by plan tier"  
SQL: SELECT plan_tier, trial_signup_rate FROM ${catalog.table} GROUP BY plan_tier

Request: "show me all the activation metrics"
SQL: SELECT avg_activation_events, total_activation_events FROM ${catalog.table}`;
}

/**
 * Get catalog for UI display (like the Data Catalog in the dashboard)
 */
export function getCatalogForDisplay(table?: "sessions" | "users"): {
  dimensions: Array<{ alias: string; description: string; type: string }>;
  measures: Array<{ alias: string; description: string; format: string }>;
} {
  const catalog = generateSemanticCatalog(table);
  
  return {
    dimensions: catalog.dimensions.map(d => ({
      alias: d.alias,
      description: d.description,
      type: d.type
    })),
    measures: catalog.measures.map(m => ({
      alias: m.alias,
      description: m.description,
      format: m.format
    }))
  };
}
