// utils/smarter/dashboard_utils/semantic-query-validator.ts
import { getSemanticMetadata, findDimensionByAlias as _findDimensionByAlias, findMeasureByAlias } from "./semantic-config.ts";

/**
 * Parsed query structure with user-friendly aliases
 */
export interface ParsedQuery {
  table: string;
  dimensions: Array<{
    alias: string;
    sourceField: string;
    transformation: string | null;
  }>;
  measures: Array<{
    alias: string;
    sourceField: string;
    aggregation: string | null;
    formula: string | null;
    type: 'aggregation' | 'formula';
  }>;
  filters: string[];
  sql: string;
}

/**
 * Validation result with corrections
 */
export interface ValidationResult {
  valid: boolean;
  errors: Array<{
    type: 'alias_as_column' | 'wrong_aggregation' | 'unknown_column' | 'missing_aggregation';
    column: string;
    message: string;
    correction: string;
    fieldMetadata?: Record<string, unknown>;
  }>;
  warnings: string[];
  correctionPrompt?: string;
}

/**
 * Visualization categorization
 */
interface CategorizedDimensions {
  temporal: string[];
  sequential: string[];
  categorical: string[];
  binary: string[];
}

interface CategorizedMeasures {
  counts: string[];
  rates: string[];
  revenue: string[];
  events: string[];
  averages: string[];
  totals: string[];
}

/**
 * Comprehensive query validation and analysis
 */
export class SemanticQueryValidator {
  private metadata: ReturnType<typeof getSemanticMetadata>;

  constructor(private table: string = "sessions") {
    this.metadata = getSemanticMetadata(table);
  }

  /**
   * Validate and parse SQL query
   */
  validate(sql: string): ValidationResult & { parsed?: ParsedQuery } {
    const errors: ValidationResult['errors'] = [];
    const warnings: string[] = [];

    // Check for invalid DuckDB functions
    const invalidFunctions = this.checkDuckDBFunctions(sql);
    if (invalidFunctions.length > 0) {
      invalidFunctions.forEach(func => {
        errors.push({
          type: 'unknown_column',
          column: func.invalid,
          message: `Invalid DuckDB function: ${func.invalid}()`,
          correction: `Use ${func.correction} instead`
        });
      });
    }

    // Extract columns from SELECT
    const selectedColumns = this.extractSelectedColumns(sql);
    const selectAliases = new Set(selectedColumns.map(c => c.alias).filter(Boolean));
    
    // Extract GROUP BY columns (dimensions)
    const groupByColumns = this.extractGroupByColumns(sql);
    
    // Validate each selected column
    selectedColumns.forEach(col => {
      const validation = this.validateColumn(col);
      if (validation.error) {
        errors.push(validation.error);
      }
    });

    // Validate GROUP BY columns (allow SELECT aliases - valid in DuckDB)
    groupByColumns.forEach(col => {
      const cleanCol = col.replace(/"/g, '').trim();
      
      // If it's a SELECT alias, it's valid
      if (selectAliases.has(cleanCol)) {
        return;
      }
      
      const validation = this.validateDimension(col);
      if (validation.error) {
        errors.push(validation.error);
      }
    });

    const valid = errors.length === 0;

    // Generate correction prompt if errors exist
    let correctionPrompt: string | undefined;
    if (!valid) {
      correctionPrompt = this.generateCorrectionPrompt(sql, errors);
    }

    // Parse query structure if valid
    let parsed: ParsedQuery | undefined;
    if (valid) {
      parsed = this.parseQuery(sql, selectedColumns, groupByColumns);
    }

    return {
      valid,
      errors,
      warnings,
      correctionPrompt,
      parsed
    };
  }

  /**
   * Check for invalid DuckDB date/time functions
   */
  private checkDuckDBFunctions(sql: string): Array<{invalid: string, correction: string}> {
    const invalid: Array<{invalid: string, correction: string}> = [];
    
    // Only check for common function name errors, not syntax
    if (/\bCURDATE\s*\(/i.test(sql)) {
      invalid.push({ invalid: 'CURDATE', correction: 'CURRENT_DATE' });
    }
    if (/\bNOW\s*\(/i.test(sql)) {
      invalid.push({ invalid: 'NOW', correction: 'CURRENT_TIMESTAMP' });
    }
    
    return invalid;
  }

  /**
   * Extract selected columns from SELECT clause
   */
  private extractSelectedColumns(sql: string): Array<{
    raw: string;
    alias: string | null;
    expression: string;
  }> {
    const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/is);
    if (!selectMatch) return [];

    return selectMatch[1].split(',').map(col => {
      col = col.trim();
      
      // Check for alias
      const aliasMatch = col.match(/(.+)\s+AS\s+(\w+)$/i);
      if (aliasMatch) {
        return {
          raw: col,
          alias: aliasMatch[2],
          expression: aliasMatch[1].trim()
        };
      }

      return {
        raw: col,
        alias: null,
        expression: col
      };
    });
  }

  /**
   * Extract GROUP BY columns
   */
  private extractGroupByColumns(sql: string): string[] {
    const groupByMatch = sql.match(/GROUP\s+BY\s+(.+?)(?=\s*(?:ORDER|LIMIT|;|$))/is);
    if (!groupByMatch) return [];

    return groupByMatch[1]
      .split(',')
      .map(col => col.trim())
      .filter(col => col && col !== '');
  }

  /**
   * Validate column - check if field names used are valid
   */
  private validateColumn(col: {
    raw: string;
    alias: string | null;
    expression: string;
  }): { error?: ValidationResult['errors'][0] } {
    const expr = col.expression;

    // Allow CASE statements (they're transformations)
    if (expr.toUpperCase().includes('CASE')) {
      // Extract field names from CASE and validate them
      const fieldNames = this.extractFieldNames(expr);
      const invalidFields = fieldNames.filter(f => !this.metadata.fields[f]);
      
      if (invalidFields.length > 0) {
        return { error: {
          type: 'unknown_column',
          column: invalidFields[0],
          message: `Unknown field "${invalidFields[0]}" in CASE statement`,
          correction: `Available fields: ${Object.keys(this.metadata.fields).join(', ')}`
        }};
      }
      return {};
    }

    // Check aggregations
    const aggMatch = expr.match(/^(SUM|AVG|COUNT|MAX|MIN)\s*\((.+)\)$/i);
    if (aggMatch) {
      const innerExpr = aggMatch[2].trim().replace(/DISTINCT\s+/i, '').trim();
      
      // Validate the base field
      if (!this.metadata.fields[innerExpr]) {
        return { error: {
          type: 'unknown_column',
          column: innerExpr,
          message: `Unknown field "${innerExpr}" in aggregation`,
          correction: `Use one of: ${Object.keys(this.metadata.fields).slice(0, 30).join(', ')}`
        }};
      }
      return {};
    }

    // Simple field reference
    const fieldName = expr.trim();
    if (!this.metadata.fields[fieldName] && !expr.includes('(')) {
      return { error: {
        type: 'unknown_column',
        column: fieldName,
        message: `Unknown field "${fieldName}"`,
        correction: `Use one of: ${Object.keys(this.metadata.fields).slice(0, 30).join(', ')}`
      }};
    }

    return {};
  }

  /**
   * Extract field names from expression
   */
  private extractFieldNames(expr: string): string[] {
    const fields: string[] = [];
    const knownFields = Object.keys(this.metadata.fields);
    
    // Remove function calls and operators to avoid false matches
    const cleanExpr = expr
      .replace(/INTERVAL\s+'[^']+'/gi, '') // Remove INTERVAL literals
      .replace(/DATE_SUB\s*\([^)]*\)/gi, '') // Remove DATE_SUB calls
      .replace(/DATE_DIFF\s*\([^)]*\)/gi, '') // Remove DATE_DIFF calls
      .replace(/CURRENT_DATE/gi, '') // Remove CURRENT_DATE
      .replace(/CURRENT_TIMESTAMP/gi, ''); // Remove CURRENT_TIMESTAMP
    
    // Look for each known field name
    knownFields.forEach(field => {
      const regex = new RegExp(`\\b${field}\\b`, 'gi');
      if (regex.test(cleanExpr)) {
        fields.push(field);
      }
    });
    
    return fields;
  }

  /**
   * Validate dimension in GROUP BY
   */
  private validateDimension(column: string): { error?: ValidationResult['errors'][0] } {
    // Allow CASE statements
    if (column.toUpperCase().includes('CASE')) {
      const fieldNames = this.extractFieldNames(column);
      const invalidFields = fieldNames.filter(f => !this.metadata.fields[f]);
      
      if (invalidFields.length > 0) {
        return { error: {
          type: 'unknown_column',
          column: invalidFields[0],
          message: `Unknown field "${invalidFields[0]}" in GROUP BY CASE`,
          correction: `Use valid field names`
        }};
      }
      return {};
    }

    // Simple field reference
    const fieldName = column.trim();
    if (!this.metadata.fields[fieldName]) {
      return { error: {
        type: 'unknown_column',
        column: fieldName,
        message: `Unknown field "${fieldName}" in GROUP BY`,
        correction: `Use one of: ${Object.keys(this.metadata.fields).slice(0, 30).join(', ')}`
      }};
    }

    return {};
  }

  /**
   * Parse valid query into structured format
   */
  private parseQuery(
    sql: string,
    selectedColumns: ReturnType<typeof this.extractSelectedColumns>,
    groupByColumns: string[]
  ): ParsedQuery {
    const dimensions: ParsedQuery['dimensions'] = [];
    const measures: ParsedQuery['measures'] = [];

    // Parse dimensions from GROUP BY
    groupByColumns.forEach(col => {
      const columnName = col.replace(/.*\./, '').trim();
      const dimConfig = this.metadata.dimensions[columnName];
      
      if (dimConfig) {
        dimensions.push({
          alias: dimConfig.alias_name,
          sourceField: columnName,
          transformation: dimConfig.transformation
        });
      }
    });

    // Parse measures from SELECT
    selectedColumns.forEach(col => {
      if (col.alias) {
        const measureInfo = findMeasureByAlias(col.alias, this.table);
        if (measureInfo) {
          measures.push({
            alias: col.alias,
            sourceField: measureInfo.sourceColumn,
            aggregation: measureInfo.type === 'aggregation' ? measureInfo.aggregationType || null : null,
            formula: measureInfo.type === 'formula' ? measureInfo.formulaSQL || null : null,
            type: measureInfo.type
          });
        }
      }
    });

    // Extract filters
    const filters: string[] = [];
    const whereMatch = sql.match(/WHERE\s+(.+?)(?:GROUP\s+BY|ORDER\s+BY|LIMIT|$)/is);
    if (whereMatch) {
      filters.push(whereMatch[1].trim());
    }

    return {
      table: this.metadata.table,
      dimensions,
      measures,
      filters,
      sql
    };
  }

  /**
   * Generate correction prompt for LLM
   */
  private generateCorrectionPrompt(_sql: string, errors: ValidationResult['errors']): string {
    const uniqueErrorTypes = new Set(errors.map(e => e.type));
    
    let prompt = ` SQL ERRORS (${errors.length})\n\n`;
    
    errors.forEach((err, idx) => {
      prompt += `${idx + 1}. ${err.message}\n`;
      prompt += `   Fix: ${err.correction}\n\n`;
    });

    // Only show relevant metadata based on error types
    const showDimensions = uniqueErrorTypes.has('alias_as_column') || errors.some(e => e.fieldMetadata?.transformation);
    const showMeasures = uniqueErrorTypes.has('wrong_aggregation') || uniqueErrorTypes.has('missing_aggregation');
    
    if (showDimensions || showMeasures) {
      prompt += ` RELEVANT MAPPINGS:\n\n`;
      
      // Show only the fields mentioned in errors
      const errorColumns = new Set(errors.map(e => e.column));
      
      if (showDimensions) {
        const relevantDims = Object.entries(this.metadata.dimensions)
          .filter(([source, dim]) => errorColumns.has(dim.alias_name) || errorColumns.has(source))
          .slice(0, 5);
        
        if (relevantDims.length > 0) {
          prompt += `Dimensions:\n`;
          relevantDims.forEach(([source, dim]) => {
            if (dim.transformation) {
              prompt += `  "${dim.alias_name}" → (${dim.transformation})\n`;
            } else {
              prompt += `  "${dim.alias_name}" → ${source}\n`;
            }
          });
          prompt += `\n`;
        }
      }
      
      if (showMeasures) {
        const relevantMeasures: Array<[string, string, string]> = [];
        Object.entries(this.metadata.measures).forEach(([source, measure]) => {
          if (measure.aggregations) {
            measure.aggregations.forEach((agg: Record<string, unknown>) => {
              const aggType = Object.keys(agg)[0];
              const aggConfig = agg[aggType] as { alias: string };
              if (errorColumns.has(aggConfig.alias) || errorColumns.has(source)) {
                relevantMeasures.push([aggConfig.alias, aggType.toUpperCase(), source]);
              }
            });
          }
        });
        
        if (relevantMeasures.length > 0) {
          prompt += `Measures:\n`;
          relevantMeasures.slice(0, 5).forEach(([alias, agg, source]) => {
            prompt += `  "${alias}" → ${agg}(${source})\n`;
          });
        }
      }
    }

    prompt += `\n CRITICAL: Use source column names in SQL, return with AS for aliases!\n`;
    prompt += `Table: ${this.metadata.table}\n`;

    return prompt;
  }

  /**
   * Analyze query for visualization recommendations
   */
  analyzeForVisualization(parsed: ParsedQuery): {
    queryType: 'kpi' | 'time_series' | 'distribution' | 'comparison' | 'funnel' | 'breakdown';
    recommendedCharts: Array<{
      type: string;
      reason: string;
      priority: number;
    }>;
    dimensionCategories: CategorizedDimensions;
    measureCategories: CategorizedMeasures;
  } {
    const dimCategories = this.categorizeDimensions(parsed.dimensions.map(d => d.alias));
    const measureCategories = this.categorizeMeasures(parsed.measures.map(m => m.alias));
    
    const queryType = this.detectQueryType(dimCategories, measureCategories);
    const recommendations = this.generateChartRecommendations(queryType, dimCategories, measureCategories);

    return {
      queryType,
      recommendedCharts: recommendations,
      dimensionCategories: dimCategories,
      measureCategories
    };
  }

  private categorizeDimensions(dimensionAliases: string[]): CategorizedDimensions {
    const temporal: string[] = [];
    const sequential: string[] = [];
    const categorical: string[] = [];
    const binary: string[] = [];

    dimensionAliases.forEach(alias => {
      // Find source field
      let sourceField: string | null = null;
      for (const [source, dimConfig] of Object.entries(this.metadata.dimensions)) {
        if (dimConfig.alias_name === alias) {
          sourceField = source;
          break;
        }
      }

      if (!sourceField) return;

      const field = this.metadata.fields[sourceField];
      const dimConfig = this.metadata.dimensions[sourceField];

      if (alias.includes('date') || alias.includes('time') || 
          field?.md_data_type === 'DATE' || field?.md_data_type.includes('TIMESTAMP')) {
        temporal.push(alias);
      } else if (dimConfig?.sort?.startsWith('custom:')) {
        sequential.push(alias);
      } else if (field?.md_data_type === 'BOOLEAN' || alias.includes('is_') || alias.includes('has_')) {
        binary.push(alias);
      } else {
        categorical.push(alias);
      }
    });

    return { temporal, sequential, categorical, binary };
  }

  private categorizeMeasures(measureAliases: string[]): CategorizedMeasures {
    const counts: string[] = [];
    const rates: string[] = [];
    const revenue: string[] = [];
    const events: string[] = [];
    const averages: string[] = [];
    const totals: string[] = [];

    measureAliases.forEach(alias => {
      if (alias.includes('_rate') || alias.includes('_ratio') || alias.includes('rate')) {
        rates.push(alias);
      } else if (alias.includes('revenue')) {
        revenue.push(alias);
      } else if (alias.includes('events') || alias.includes('event')) {
        events.push(alias);
      } else if (alias.startsWith('avg_') || alias.includes('_avg')) {
        averages.push(alias);
      } else if (alias.includes('count') || alias.includes('sessions') || alias.includes('visitors') || alias.includes('users')) {
        counts.push(alias);
      } else {
        totals.push(alias);
      }
    });

    return { counts, rates, revenue, events, averages, totals };
  }

  private detectQueryType(dims: CategorizedDimensions, measures: CategorizedMeasures): 'kpi' | 'time_series' | 'distribution' | 'comparison' | 'funnel' | 'breakdown' {
    const totalDims = dims.temporal.length + dims.sequential.length + dims.categorical.length + dims.binary.length;
    
    if (totalDims === 0) return 'kpi';
    if (dims.temporal.length > 0) return 'time_series';
    if (dims.sequential.length > 0) return 'funnel';
    if (totalDims > 1) return 'breakdown';
    if (measures.rates.length > 0 || Object.values(measures).flat().length > 1) return 'comparison';
    return 'distribution';
  }

  private generateChartRecommendations(
    queryType: ReturnType<typeof this.detectQueryType>,
    _dims: CategorizedDimensions,
    _measures: CategorizedMeasures
  ) {
    const recs: Array<{ type: string; reason: string; priority: number }> = [];

    switch (queryType) {
      case 'kpi':
        recs.push({ type: 'bar', reason: 'Single metric - horizontal bar', priority: 1 });
        break;
      case 'time_series':
        recs.push({ type: 'line', reason: 'Time dimension - line shows trends', priority: 1 });
        recs.push({ type: 'area', reason: 'Time dimension - area shows volume', priority: 2 });
        break;
      case 'funnel':
        recs.push({ type: 'funnel', reason: 'Sequential dimension - funnel shows progression', priority: 1 });
        recs.push({ type: 'bar', reason: 'Ordered categories alternative', priority: 2 });
        break;
      case 'distribution':
        recs.push({ type: 'pie', reason: 'Distribution - pie shows proportions', priority: 1 });
        recs.push({ type: 'bar', reason: 'Distribution - bar for comparison', priority: 2 });
        break;
      case 'comparison':
        recs.push({ type: 'bar', reason: 'Compare metrics across categories', priority: 1 });
        break;
      case 'breakdown':
        recs.push({ type: 'heatmap', reason: 'Multiple dimensions - heatmap shows relationships', priority: 1 });
        break;
    }

    return recs;
  }
}

/**
 * Factory function
 */
export function createQueryValidator(table: string = "sessions"): SemanticQueryValidator {
  return new SemanticQueryValidator(table);
}
