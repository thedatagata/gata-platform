// utils/smarter/sql-column-extractor.ts
import { getSemanticMetadata } from "../dashboard_utils/semantic-config.ts";

/**
 * Extract column names from SQL SELECT clause
 * Handles aliases like "column as alias_name"
 */
export function extractColumnsFromSQL(sql: string): string[] {
  const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/is);
  if (!selectMatch) return [];
  
  const columns = selectMatch[1]
    .split(',')
    .map(col => {
      col = col.trim();
      
      // Extract alias if present (... AS alias_name)
      const aliasMatch = col.match(/\s+AS\s+(\w+)$/i);
      if (aliasMatch) {
        return aliasMatch[1];
      }
      
      // If no alias, extract just the column name (remove aggregations, table prefix)
      col = col.replace(/^(SUM|AVG|COUNT|MAX|MIN|COUNT_DISTINCT|ANY_VALUE)\s*\(/i, '');
      col = col.replace(/\)$/, '');
      col = col.replace(/.*\./, '');
      
      return col.trim();
    })
    .filter(col => col && col !== '*');
  
  return columns;
}

/**
 * Map alias name back to source field key
 * Returns null if not found as an alias
 */
function findSourceFieldByAlias(aliasName: string, table: "sessions" | "users"): {
  sourceField: string;
  type: 'dimension' | 'measure';
  metadata: any;
} | null {
  const metadata = getSemanticMetadata(table);
  
  // Check dimensions
  for (const [sourceField, dimConfig] of Object.entries(metadata.dimensions)) {
    if (dimConfig.alias_name === aliasName) {
      return {
        sourceField,
        type: 'dimension',
        metadata: {
          alias: dimConfig.alias_name,
          transformation: dimConfig.transformation,
          fieldInfo: metadata.fields[sourceField]
        }
      };
    }
  }
  
  // Check measures (aggregations)
  for (const [sourceField, measureConfig] of Object.entries(metadata.measures)) {
    if (measureConfig.aggregations && Array.isArray(measureConfig.aggregations)) {
      for (const aggObj of measureConfig.aggregations) {
        const aggType = Object.keys(aggObj)[0];
        const aggConfig = aggObj[aggType];
        
        if (aggConfig.alias === aliasName) {
          return {
            sourceField,
            type: 'measure',
            metadata: {
              alias: aggConfig.alias,
              aggregationType: aggType,
              fieldInfo: metadata.fields[sourceField],
              description: measureConfig.description
            }
          };
        }
      }
    }
    
    // Check formulas
    if (measureConfig.formula && typeof measureConfig.formula === 'object') {
      if (measureConfig.formula[aliasName]) {
        return {
          sourceField,
          type: 'measure',
          metadata: {
            alias: aliasName,
            isFormula: true,
            formulaSQL: measureConfig.formula[aliasName].sql,
            fieldInfo: metadata.fields[sourceField],
            description: measureConfig.description
          }
        };
      }
    }
  }
  
  return null;
}

/**
 * Validate SQL columns against semantic metadata
 * Returns validation results with corrections for aliases used incorrectly
 */
export function validateSQLColumns(
  sql: string,
  table: "sessions" | "users" = "sessions"
): {
  valid: boolean;
  validFields: string[];
  invalidColumns: string[];
  aliasErrors: Array<{
    usedAlias: string;
    sourceField: string;
    type: 'dimension' | 'measure';
    correction: string;
  }>;
  unknownColumns: string[];
  suggestions: string[];
} {
  const columns = extractColumnsFromSQL(sql);
  const metadata = getSemanticMetadata(table);
  
  const validFields: string[] = [];
  const aliasErrors: Array<{
    usedAlias: string;
    sourceField: string;
    type: 'dimension' | 'measure';
    correction: string;
  }> = [];
  const unknownColumns: string[] = [];
  
  columns.forEach(col => {
    // Check if column exists in fields (source columns)
    if (metadata.fields[col]) {
      validFields.push(col);
    } else {
      // Check if it's an alias that maps to a source field
      const aliasMapping = findSourceFieldByAlias(col, table);
      
      if (aliasMapping) {
        // Found it - this is an alias used incorrectly
        let correction: string;
        
        if (aliasMapping.type === 'dimension') {
          if (aliasMapping.metadata.transformation) {
            correction = `Use: (${aliasMapping.metadata.transformation}) AS ${col}`;
          } else {
            correction = `Use source column: ${aliasMapping.sourceField} (not alias ${col})`;
          }
        } else {
          // Measure
          if (aliasMapping.metadata.isFormula) {
            correction = `Use: (${aliasMapping.metadata.formulaSQL}) AS ${col}`;
          } else {
            correction = `Use: ${aliasMapping.metadata.aggregationType.toUpperCase()}(${aliasMapping.sourceField}) AS ${col}`;
          }
        }
        
        aliasErrors.push({
          usedAlias: col,
          sourceField: aliasMapping.sourceField,
          type: aliasMapping.type,
          correction
        });
      } else {
        // Not a field and not an alias - completely unknown
        unknownColumns.push(col);
      }
    }
  });
  
  const suggestions: string[] = [];
  
  if (aliasErrors.length > 0) {
    suggestions.push("  You used alias names instead of source columns:");
    aliasErrors.forEach(err => {
      suggestions.push(`  • ${err.usedAlias} → ${err.correction}`);
    });
  }
  
  if (unknownColumns.length > 0) {
    suggestions.push(` Unknown columns: ${unknownColumns.join(", ")}`);
    
    // Try to find similar field names
    const allFields = Object.keys(metadata.fields);
    unknownColumns.forEach(unknownCol => {
      const similar = allFields.filter(f => 
        f.toLowerCase().includes(unknownCol.toLowerCase()) || 
        unknownCol.toLowerCase().includes(f.toLowerCase())
      );
      if (similar.length > 0) {
        suggestions.push(`   Did you mean: ${similar.slice(0, 3).join(", ")}?`);
      }
    });
  }
  
  const valid = aliasErrors.length === 0 && unknownColumns.length === 0;
  
  return {
    valid,
    validFields,
    invalidColumns: [...aliasErrors.map(e => e.usedAlias), ...unknownColumns],
    aliasErrors,
    unknownColumns,
    suggestions
  };
}

/**
 * Generate correction prompt when aliases are used incorrectly
 * Provides field metadata to help LLM generate correct SQL
 */
export function generateCorrectionPrompt(
  originalPrompt: string,
  validation: ReturnType<typeof validateSQLColumns>,
  table: "sessions" | "users"
): string {
  const metadata = getSemanticMetadata(table);
  
  let prompt = `Your previous SQL query had errors:\n\n`;
  
  // Explain alias errors with field metadata
  if (validation.aliasErrors.length > 0) {
    prompt += ` ALIAS ERRORS - You used user-facing alias names, but SQL needs source column names with transformations:\n\n`;
    
    validation.aliasErrors.forEach(err => {
      const sourceMapping = findSourceFieldByAlias(err.usedAlias, table);
      if (!sourceMapping) return;
      
      prompt += `Field: ${err.usedAlias}\n`;
      prompt += `  Source Column: ${err.sourceField}\n`;
      
      if (err.type === 'dimension' && sourceMapping.metadata.transformation) {
        prompt += `  SQL: (${sourceMapping.metadata.transformation}) AS ${err.usedAlias}\n`;
      } else if (err.type === 'measure') {
        if (sourceMapping.metadata.isFormula) {
          prompt += `  SQL: (${sourceMapping.metadata.formulaSQL}) AS ${err.usedAlias}\n`;
        } else {
          prompt += `  SQL: ${sourceMapping.metadata.aggregationType.toUpperCase()}(${err.sourceField}) AS ${err.usedAlias}\n`;
        }
      } else {
        prompt += `  SQL: ${err.sourceField} AS ${err.usedAlias}\n`;
      }
      
      if (sourceMapping.metadata.fieldInfo) {
        prompt += `  Description: ${sourceMapping.metadata.fieldInfo.description}\n`;
      }
      prompt += `\n`;
    });
  }
  
  // Explain unknown columns
  if (validation.unknownColumns.length > 0) {
    prompt += ` UNKNOWN COLUMNS: ${validation.unknownColumns.join(", ")}\n`;
    prompt += `These don't exist in the catalog. Available fields: ${Object.keys(metadata.fields).slice(0, 15).join(", ")}...\n\n`;
  }
  
  prompt += `Original request: "${originalPrompt}"\n\n`;
  prompt += `Table: ${metadata.table}\n\n`;
  prompt += `Generate corrected SQL using source column names with proper transformations/aggregations.`;
  
  return prompt;
}
