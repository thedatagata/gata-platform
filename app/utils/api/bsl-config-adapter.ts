// BSL Config Adapter — converts BSL YAML config format to SemanticMetadata

import type { SemanticMetadata } from "../smarter/dashboard_utils/semantic-config.ts";
import type { ModelDetail, BSLConfig } from "./platform-api-client.ts";


/**
 * Map BSL dimension type to DuckDB data type
 */
function mapTypeToDuckDB(bslType: string): string {
  switch (bslType) {
    case "string": return "VARCHAR";
    case "date": return "DATE";
    case "timestamp": return "TIMESTAMP";
    case "timestamp_epoch": return "BIGINT";
    case "boolean": return "BOOLEAN";
    case "number": return "DOUBLE";
    default: return "VARCHAR";
  }
}

/**
 * Infer data_type_category from BSL type
 */
function inferCategory(bslType: string): string {
  switch (bslType) {
    case "date":
    case "timestamp":
    case "timestamp_epoch":
      return "temporal";
    case "boolean":
      return "boolean";
    case "number":
      return "numeric";
    case "string":
    default:
      return "categorical";
  }
}

/**
 * Map BSL aggregation string to the format expected by SemanticMetadata measures
 */
function mapAgg(agg: string): string {
  switch (agg) {
    case "sum": return "sum";
    case "avg": return "avg";
    case "count": return "count";
    case "count_distinct": return "count_distinct";
    case "max": return "max";
    case "min": return "min";
    default: return "sum";
  }
}


/**
 * Convert a BSL ModelDetail into the frontend's SemanticMetadata format.
 * This allows all existing frontend code (SemanticProfiler, SemanticQueryValidator,
 * WebLLMSemanticHandler, CustomDataDashboard) to work with star schema models.
 */
export function adaptBSLModelToSemanticMetadata(model: ModelDetail): SemanticMetadata {
  const fields: SemanticMetadata["fields"] = {};
  const dimensions: SemanticMetadata["dimensions"] = {};
  const measures: SemanticMetadata["measures"] = {};

  // 1. Build fields + dimensions from BSL dimensions
  for (const dim of model.dimensions) {
    fields[dim.name] = {
      description: `Dimension: ${dim.name}`,
      md_data_type: mapTypeToDuckDB(dim.type),
      ingest_data_type: dim.type,
      sanitize: false,
      data_type_category: inferCategory(dim.type),
      members: null,
    };

    dimensions[dim.name] = {
      alias_name: dim.name,
      transformation: null,
    };
  }

  // 2. Build fields + measures from BSL measures
  for (const m of model.measures) {
    fields[m.name] = {
      description: `Measure: ${m.name}`,
      md_data_type: mapTypeToDuckDB(m.type),
      ingest_data_type: m.type,
      sanitize: false,
      data_type_category: "numeric",
      members: null,
    };

    const aggKey = mapAgg(m.agg);
    measures[m.name] = {
      aggregations: [
        { [aggKey]: { alias: m.label || m.name, format: undefined } },
      ],
      description: `Measure: ${m.name}`,
    };
  }

  // 3. Build measures from BSL calculated_measures
  for (const cm of model.calculated_measures) {
    measures[cm.name] = {
      formula: {
        [cm.name]: {
          sql: cm.sql,
          description: cm.label,
          format: cm.format,
        },
      },
      description: cm.label,
    };
  }

  return {
    table: model.name,
    description: model.description,
    fields,
    dimensions,
    measures,
  };
}


/**
 * Convert a full BSL config (all models) into a Map of SemanticMetadata keyed by model name.
 */
export function adaptBSLConfigToSemanticMetadataMap(config: BSLConfig): Map<string, SemanticMetadata> {
  const map = new Map<string, SemanticMetadata>();
  for (const model of config.models) {
    map.set(model.name, adaptBSLModelToSemanticMetadata(model));
  }
  return map;
}
