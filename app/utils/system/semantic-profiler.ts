/**
 * Utility for profiling DuckDB tables and generating initial semantic metadata
 */

export interface SemanticField {
  description: string;
  md_data_type: string;
  ingest_data_type: string;
  sanitize: boolean;
  data_type_category: 'categorical' | 'numerical' | 'temporal' | 'identifier' | 'continuous';
  members: (string | number | boolean)[] | null;
}

export interface SemanticAggregation {
  [type: string]: {
    alias: string;
    format?: string;
    currency?: string;
    decimals?: number;
    description?: string;
  };
}

export interface SemanticFormula {
  sql: string;
  description?: string;
  format?: string;
  decimals?: number;
}

export interface SemanticLayer {
  table: string;
  description: string;
  fields: Record<string, SemanticField>;
  dimensions: Record<string, { alias_name: string; transformation: string | null }>;
  measures: Record<string, { aggregations: SemanticAggregation[]; formula: Record<string, SemanticFormula> }>;
}

/**
 * Helper to prettify column names
 */
export const toTitleCase = (str: string) => str.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());

/**
 * Returns default dimensions for a field based on its category
 */
export function getDefaultDimensionsForField(colName: string, category: string, transformation: string | null = null): Record<string, { alias_name: string, transformation: string | null }> {
  if (category === 'categorical' || category === 'temporal' || category === 'identifier') {
    return {
      [colName]: {
        alias_name: toTitleCase(colName),
        transformation
      }
    };
  }
  return {};
}

/**
 * Returns default measures for a field based on its category
 */
export function getDefaultMeasuresForField(colName: string, category: string): Record<string, { aggregations: SemanticAggregation[]; formula: Record<string, SemanticFormula> }> {
  const colNameLower = colName.toLowerCase();
  const titleName = toTitleCase(colName);
  
  if (category === 'identifier') {
     return {
      [colName]: {
          aggregations: [],
          formula: {
              [`unique_${colName}`]: {
                  sql: `COUNT(DISTINCT ${colName})`,
                  format: "number",
                  decimals: 0,
                  description: `Unique count of ${titleName}`
              }
          }
      }
    };
  }

  if (category === 'numerical' || category === 'continuous') {
    const isRevenue = colNameLower.includes('revenue') || colNameLower.includes('amount') || colNameLower.includes('price');
    const format = isRevenue ? 'currency' : 'number';
    const currency = isRevenue ? 'USD' : undefined;

    return {
        [colName]: {
            aggregations: [
                { sum: { alias: `total_${colName}`, format, currency, decimals: isRevenue ? 2 : 0, description: `Total ${titleName}` } },
                { avg: { alias: `avg_${colName}`, format, currency, decimals: 1, description: `Average ${titleName}` } },
                { min: { alias: `min_${colName}`, format, currency, decimals: 0, description: `Minimum ${titleName}` } },
                { max: { alias: `max_${colName}`, format, currency, decimals: 0, description: `Maximum ${titleName}` } },
                { count: { alias: `count_${colName}`, format: 'number', decimals: 0, description: `Total count of ${titleName}` } },
            ],
            formula: {
                [`unique_${colName}_vals`]: {
                    sql: `COUNT(DISTINCT ${colName})`,
                    format: "number",
                    decimals: 0,
                    description: `Unique values of ${titleName}`
                }
            }
        }
    };
  }

  return {};
}

/**
 * Converts a Uint8Array (little-endian) to a number (up to 53 bits of precision)
 */
function uint8ArrayToNumber(arr: Uint8Array): number {
  let result = 0;
  for (let i = 0; i < Math.min(8, arr.length); i++) {
    result += arr[i] * Math.pow(256, i);
  }
  return result;
}

/**
 * Sanitizes values from DuckDB to be JSON-serializable and JS-friendly
 */
function sanitizeValue(value: unknown): unknown {
  if (typeof value === 'bigint') {
    return Number(value);
  } else if (value instanceof Uint8Array) {
    return uint8ArrayToNumber(value);
  } else if (value instanceof Date) {
    if (isNaN(value.getTime())) return null;
    return value.toISOString();
  }
  return value;
}

/**
 * Profiles a table and returns a draft semantic layer configuration
 */
export async function profileTable(db: { query?: (sql: string) => Promise<unknown>; evaluateQuery?: (sql: string) => Promise<unknown> }, tableName: string): Promise<SemanticLayer> {

  console.log(` Profiling table: ${tableName}`);
  
  // Helper to extract rows from different DuckDB result formats
  const getRows = (result: unknown): Record<string, unknown>[] => {
    let rawRows: Record<string, unknown>[] = [];
    const res = result as Record<string, unknown>;
    // MotherDuck evaluateQuery result
    if (res && res.data && typeof (res.data as any).toArray === 'function') {
      rawRows = (res.data as any).toArray().map((r: any) => typeof r.toJSON === 'function' ? r.toJSON() : r);
    } else if (res && typeof (res as any).toArray === 'function') {
      // Standard DuckDB query result (Table)
      rawRows = (res as any).toArray().map((r: any) => typeof r.toJSON === 'function' ? r.toJSON() : r);
    } else if (res && res.data && typeof (res.data as any).toRows === 'function') {
        rawRows = (res.data as any).toRows();
    } else if (res && typeof (res as any).toRows === 'function') {
        rawRows = (res as any).toRows();
    } else if (Array.isArray(result)) {
        rawRows = result as Record<string, unknown>[];
    } else if (res && Array.isArray(res.data)) {
        rawRows = res.data as Record<string, unknown>[];
    }

    // Sanitize values (handle BigInt, Uint8Array, etc.)
    return rawRows.map(row => {
      if (!row || typeof row !== 'object') return row as Record<string, unknown>;
      const sanitized: Record<string, unknown> = {};
      for (const [key, val] of Object.entries(row)) {
        sanitized[key] = sanitizeValue(val);
      }
      return sanitized;
    }) as Record<string, unknown>[];
  };

  const queryMethod = db.evaluateQuery ? 'evaluateQuery' : 'query';
  const queryFn = (db as any)[queryMethod].bind(db);

  // 1. Get statistics using SUMMARIZE
  const summaryResult = await queryFn(`SUMMARIZE ${tableName}`);
  const summaryRows = getRows(summaryResult) as Record<string, unknown>[];

  const fields: Record<string, SemanticField> = {};
  const dimensions: Record<string, { alias_name: string; transformation: string | null }> = {};
  const measures: Record<string, { aggregations: any[]; formula: any }> = {};

  // Helper to prettify column names
  const toTitleCase = (str: string) => str.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());

  for (const row of summaryRows) {
    const colName = row.column_name as string;
    const colType = (row.column_type as string || "").toUpperCase();
    const approxDistinct = Number(row.approx_unique || 0);
    const colNameLower = colName.toLowerCase();
    
    // 1. Category and Type Inference
    let category: SemanticField['data_type_category'] = 'categorical';
    let ingestType = 'string';

    const isInteger = ['INT', 'HUGEINT'].some(t => colType.includes(t));
    const isFloat = ['FLOAT', 'DOUBLE', 'DECIMAL', 'REAL', 'NUMERIC'].some(t => colType.includes(t));
    const isNumeric = isInteger || isFloat;
    const isDate = ['DATE', 'TIMESTAMP'].some(t => colType.includes(t));
    const isTime = ['TIME'].some(t => colType.includes(t));
    const isId = colNameLower.endsWith('_id') || colNameLower.endsWith('id') || colNameLower.endsWith('_key') || colNameLower === 'id' || colNameLower === 'pk';
    
    // Check if it's a string that could be numeric or a date
    let isCastableToNumeric = false;
    let isCastableToDate = false;
    let isCastableToTime = false;

    if (!isNumeric && !isDate && !isTime && (colType.includes('VARCHAR') || colType.includes('STRING'))) {
      try {
        // 0. Check strict time format (e.g. "6:44 PM", "18:44") to avoid confusion with Numerics or full Dates
        // We use a regex check for common time formats
        const timeCheck = await queryFn(`SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(regexp_matches("${colName}", '^(?:(?:[01]?\\d|2[0-3]):[0-5]\\d(?:\\s?[AP]M)?)$')) as is_time FROM ${tableName} WHERE "${colName}" IS NOT NULL`);
        const rowsT = getRows(timeCheck);
        isCastableToTime = !!(rowsT[0] as Record<string, unknown>)?.is_time;

        if (isCastableToTime) {
           console.log(` Auto-detected time string: ${colName}`);
        } else {
             // 1. Check Numeric (Ensuring at least one non-null row exists)
            const castCheck = await queryFn(`SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(TRY_CAST("${colName}" AS DOUBLE)) as is_numeric FROM ${tableName} WHERE "${colName}" IS NOT NULL`);
            const rowsN = getRows(castCheck);
            isCastableToNumeric = !!(rowsN[0] as Record<string, unknown>)?.is_numeric;

            // 2. Check Date (if not numeric)
            if (!isCastableToNumeric) {
              // Standard DuckDB cast (usually YYYY-MM-DD)
              const dateCheck = await queryFn(`SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(TRY_CAST("${colName}" AS DATE)) as is_date FROM ${tableName} WHERE "${colName}" IS NOT NULL`);
              const rowsD = getRows(dateCheck);
              isCastableToDate = !!(rowsD[0] as Record<string, unknown>)?.is_date;

              // If standard cast fails, try common formats like M/D/YYYY or D/M/YYYY
              if (!isCastableToDate) {
                const commonFormats = ['%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%m-%d-%Y', '%Y-%m-%d'];
                for (const fmt of commonFormats) {
                  try {
                    const fmtCheck = await queryFn(`SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(try_strptime("${colName}", '${fmt}')) as is_fmt FROM ${tableName} WHERE "${colName}" IS NOT NULL`);
                    const rowsF = getRows(fmtCheck);
                    if (!!(rowsF[0] as Record<string, unknown>)?.is_fmt) {
                      isCastableToDate = true;
                      console.log(` Detected date format ${fmt} for ${colName}`);
                      break;
                    }
                  } catch (_err) {
                    // Ignore format errors
                  }
                }
              }
            }
        }
      } catch (e) {
        console.warn(`Failed to check if ${colName} is castable:`, e);
      }
    }

    // Keywords that strongly suggest a field is a measure even if cardinality is low
    const isMeasureKeyword = ['count', 'sum', 'total', 'amount', 'price', 'revenue', 'value', 'score', 'quantity', 'rate', 'ratio', 'percent', 'pct', 'seconds', 'days', 'hours', 'ms', 'duration', 'revenue', 'cost', 'fee', 'tax'].some(kw => colNameLower.includes(kw));

    if ((isNumeric || isCastableToNumeric) && !isCastableToTime) {
      ingestType = 'number';
      if (isCastableToNumeric) {
        console.log(` Auto-detected numerical string: ${colName}`);
      }
      // Heuristic: If it has many values, or is clearly an ID, or has a measure-like name, it's numerical/continuous.
      // Otherwise, if cardinality is low (<= 14), we assume it's a categorical factor/enum.
      const looksLikeMeasure = approxDistinct > 14 || isId || isMeasureKeyword;
      
      if (looksLikeMeasure) {
        category = isFloat ? 'continuous' : 'numerical';
        if (isId) category = 'identifier';
      } else {
        category = 'categorical';
      }
    } else if (isDate || isCastableToDate) {
      category = 'temporal';
      ingestType = isDate ? 'timestamp' : 'string';
      if (isCastableToDate) {
        console.log(` Auto-detected temporal string (Date): ${colName}`);
      }
    } else if (isTime || isCastableToTime) {
       // We treat pure time columns as categorical for now, or a separate temporal type if supported later
       // But critically, we do NOT want them to be the primary 'temporal' column for "Last 90 Days" analysis
       category = 'categorical'; // Downgrade to categorical to avoid it grabbing the spotlight
       ingestType = 'string';
       console.log(` Auto-detected time string (Downgrading to Categorical): ${colName}`);
    } else if (colType.includes('BOOL')) {
      category = 'categorical';
      ingestType = 'boolean';
    }

    // 2. Fetch members for categorical fields
    let members: (string | number | boolean)[] | null = null;
    if (category === 'categorical' && approxDistinct < 50) {
      try {
        const distinctResult = await queryFn(`SELECT DISTINCT ${colName} FROM ${tableName} WHERE ${colName} IS NOT NULL LIMIT 10`);
        members = (getRows(distinctResult) as Record<string, unknown>[]).map((r) => r[colName] as (string | number | boolean));
      } catch (e) {
        console.warn(`Failed to fetch members for ${colName}:`, e);
      }
    }

    fields[colName] = {
      description: `${toTitleCase(colName)} auto-detected from ${tableName}`,
      md_data_type: colType,
      ingest_data_type: ingestType,
      sanitize: false,
      data_type_category: category,
      members: members
    };

    // 3. Strict Semantic Mapping Rules
    const fieldDimensions = getDefaultDimensionsForField(colName, category);
    const fieldMeasures = getDefaultMeasuresForField(colName, category);
    
    Object.assign(dimensions, fieldDimensions);
    Object.assign(measures, fieldMeasures);
  }

  return {
    table: tableName,
    description: `AI-inferred semantic layer for ${tableName} (${summaryRows.length} fields detected)`,
    fields,
    dimensions,
    measures
  };
}

