// utils/smarter/query-response-analyzer.ts
import { getSemanticMetadata } from "./semantic-config.ts";

/**
 * Categorized dimensions for smart visualization
 */
export interface CategorizedDimensions {
  temporal: string[];      // session_date, first_event_date, last_event_date
  sequential: string[];    // max_lifecycle_stage (ordered categories)
  categorical: string[];   // traffic_source, plan_tier (unordered categories)
  binary: string[];        // has_activated, is_active_7d
}

/**
 * Categorized measures for smart visualization
 */
export interface CategorizedMeasures {
  counts: string[];        // session_count, user_count, unique_visitors
  rates: string[];         // activation_rate, trial_signup_rate, paying_customer_rate
  revenue: string[];       // total_revenue, avg_revenue_per_session
  events: string[];        // interest_events, consideration_events
  averages: string[];      // avg_events_per_session, avg_days_active
  totals: string[];        // total_lifetime_events, total_sessions
}

/**
 * Visualization recommendation based on categorized query
 */
export interface VisualizationSpec {
  queryType: "kpi" | "time_series" | "distribution" | "comparison" | "funnel" | "breakdown";
  recommendedCharts: Array<{
    type: "bar" | "line" | "area" | "pie" | "funnel" | "scatter" | "heatmap";
    priority: number;
    reason: string;
    config?: Record<string, unknown>;
  }>;
  dimensionCategories: CategorizedDimensions;
  measureCategories: CategorizedMeasures;
  warnings: string[];
  suggestions: string[];
}

export class QueryResponseAnalyzer {
  private metadata = getSemanticMetadata();

  constructor(table?: string) {
    this.metadata = getSemanticMetadata(table);
  }

  /**
   * Analyze semantic query response and generate visualization recommendations
   */
  analyze(queryResponse: {
    dimensions: string[];
    measures: string[];
    filters?: string[];
    explanation?: string;
  }): VisualizationSpec {
    const dimCategories = this.categorizeDimensions(queryResponse.dimensions);
    const measureCategories = this.categorizeMeasures(queryResponse.measures);
    
    const queryType = this.detectQueryType(dimCategories, measureCategories);
    const recommendations = this.generateRecommendations(queryType, dimCategories, measureCategories);
    
    const warnings: string[] = [];
    const suggestions: string[] = [];

    // Detect potential issues
    if (queryResponse.dimensions.length === 0 && queryResponse.measures.length > 1) {
      warnings.push("No dimensions - comparing multiple metrics without grouping");
      suggestions.push("Consider adding a dimension like traffic_source or session_date");
    }

    if (dimCategories.temporal.length > 0 && queryResponse.measures.length > 0) {
      suggestions.push("Time series detected - line/area charts recommended");
    }

    if (dimCategories.sequential.length > 0) {
      suggestions.push(`Sequential dimension detected: ${dimCategories.sequential[0]} - consider funnel visualization`);
    }

    if (measureCategories.rates.length > 0) {
      suggestions.push("Rate/percentage measures detected - ensure y-axis formatted as percentage");
    }

    return {
      queryType,
      recommendedCharts: recommendations,
      dimensionCategories: dimCategories,
      measureCategories,
      warnings,
      suggestions
    };
  }

  /**
   * Categorize dimensions based on metadata
   */
  private categorizeDimensions(dimensionAliases: string[]): CategorizedDimensions {
    const temporal: string[] = [];
    const sequential: string[] = [];
    const categorical: string[] = [];
    const binary: string[] = [];

    dimensionAliases.forEach(alias => {
      // Find the source column for this alias
      let sourceColumn: string | null = null;
      let dimConfig: { sort?: string; transformation?: string | null } | null = null;

      for (const [col, config] of Object.entries(this.metadata.dimensions)) {
        if (config.alias_name === alias) {
          sourceColumn = col;
          dimConfig = config as { sort?: string; transformation?: string | null };
          break;
        }
      }

      if (!sourceColumn || !dimConfig) return;

      const field = this.metadata.fields[sourceColumn];
      if (!field) return;

      // Temporal - check if dimension name or field type indicates time
      if (alias.includes('date') || alias.includes('time') || alias.includes('month') || alias.includes('weekday') || 
          field.md_data_type === 'DATE' || field.md_data_type.includes('TIMESTAMP')) {
        temporal.push(alias);
      }
      // Sequential (ordinal with custom sort indicating order)
      else if (dimConfig.sort?.startsWith("custom:")) {
        sequential.push(alias);
      }
      // Binary (has transformation and likely 2 members)
      else if (dimConfig.transformation && (
        field.md_data_type === 'BOOLEAN' || 
        field.md_data_type === 'INTEGER' ||
        alias.includes('is_') ||
        alias.includes('has_')
      )) {
        binary.push(alias);
      }
      // Categorical
      else if (field.data_type_category === "categorical" || field.members) {
        categorical.push(alias);
      }
    });

    return { temporal, sequential, categorical, binary };
  }

  /**
   * Categorize measures based on naming patterns
   */
  private categorizeMeasures(measures: string[]): CategorizedMeasures {
    const counts: string[] = [];
    const rates: string[] = [];
    const revenue: string[] = [];
    const events: string[] = [];
    const averages: string[] = [];
    const totals: string[] = [];

    measures.forEach(measure => {
      if (measure.includes("_rate") || measure.includes("_ratio") || measure.includes("rate")) {
        rates.push(measure);
      } else if (measure.includes("revenue")) {
        revenue.push(measure);
      } else if (measure.includes("events") || measure.includes("event")) {
        events.push(measure);
      } else if (measure.startsWith("avg_") || measure.includes("_avg")) {
        averages.push(measure);
      } else if (measure.includes("count") || measure.includes("sessions") || measure.includes("visitors") || measure.includes("users")) {
        counts.push(measure);
      } else if (measure.startsWith("total_") || measure.includes("_total")) {
        totals.push(measure);
      } else {
        // Default to totals
        totals.push(measure);
      }
    });

    return { counts, rates, revenue, events, averages, totals };
  }

  /**
   * Detect query type from categorized dimensions/measures
   */
  private detectQueryType(
    dims: CategorizedDimensions,
    measures: CategorizedMeasures
  ): VisualizationSpec["queryType"] {
    // No dimensions = KPI
    if (dims.temporal.length === 0 && dims.sequential.length === 0 && 
        dims.categorical.length === 0 && dims.binary.length === 0) {
      return "kpi";
    }

    // Temporal dimension = time series
    if (dims.temporal.length > 0) {
      return "time_series";
    }

    // Sequential dimension (lifecycle stages) = funnel
    if (dims.sequential.length > 0) {
      return "funnel";
    }

    // Multiple categorical dimensions = breakdown
    if (dims.categorical.length > 1 || (dims.categorical.length === 1 && dims.binary.length >= 1)) {
      return "breakdown";
    }

    // Single categorical = distribution or comparison
    if (dims.categorical.length === 1 || dims.binary.length === 1) {
      if (measures.rates.length > 0 || measures.counts.length > 1) {
        return "comparison";
      }
      return "distribution";
    }

    return "comparison";
  }

  /**
   * Generate chart recommendations based on query type
   */
  private generateRecommendations(
    queryType: VisualizationSpec["queryType"],
    dims: CategorizedDimensions,
    measures: CategorizedMeasures
  ): VisualizationSpec["recommendedCharts"] {
    const recommendations: VisualizationSpec["recommendedCharts"] = [];

    switch (queryType) {
      case "kpi":
        recommendations.push({
          type: "bar",
          priority: 1,
          reason: "Single metric comparison - horizontal bar chart",
          config: { orientation: "h" }
        });
        break;

      case "time_series":
        recommendations.push({
          type: "line",
          priority: 1,
          reason: "Temporal dimension - line chart shows trends",
          config: { x: dims.temporal[0] }
        });
        recommendations.push({
          type: "area",
          priority: 2,
          reason: "Temporal dimension - area chart shows volume",
          config: { x: dims.temporal[0] }
        });
        break;

      case "funnel":
        recommendations.push({
          type: "funnel",
          priority: 1,
          reason: "Sequential dimension detected - funnel shows progression",
          config: { dimension: dims.sequential[0] }
        });
        recommendations.push({
          type: "bar",
          priority: 2,
          reason: "Ordered categories - bar chart alternative",
          config: { orientation: "h" }
        });
        break;

      case "distribution":
        if (measures.revenue.length > 0) {
          recommendations.push({
            type: "pie",
            priority: 1,
            reason: "Revenue distribution - pie chart shows proportions"
          });
        }
        recommendations.push({
          type: "bar",
          priority: 2,
          reason: "Distribution - bar chart for comparison",
          config: { orientation: "h" }
        });
        break;

      case "comparison":
        recommendations.push({
          type: "bar",
          priority: 1,
          reason: "Comparing metrics across categories",
          config: { barmode: "group" }
        });
        break;

      case "breakdown":
        recommendations.push({
          type: "heatmap",
          priority: 1,
          reason: "Multiple dimensions - heatmap shows relationships"
        });
        recommendations.push({
          type: "bar",
          priority: 2,
          reason: "Multi-dimensional - stacked bars",
          config: { barmode: "stack" }
        });
        break;
    }

    return recommendations;
  }

  /**
   * Generate prompt enhancement for WebLLM based on analysis
   */
  generatePromptEnhancement(spec: VisualizationSpec): string {
    const parts: string[] = [];

    parts.push(`Query Type: ${spec.queryType}`);

    if (spec.dimensionCategories.temporal.length > 0) {
      parts.push(`Temporal: ${spec.dimensionCategories.temporal.join(", ")}`);
    }
    if (spec.dimensionCategories.sequential.length > 0) {
      parts.push(`Sequential: ${spec.dimensionCategories.sequential.join(", ")}`);
    }
    if (spec.dimensionCategories.categorical.length > 0) {
      parts.push(`Categories: ${spec.dimensionCategories.categorical.join(", ")}`);
    }

    if (spec.recommendedCharts.length > 0) {
      const topChart = spec.recommendedCharts[0];
      parts.push(`Recommended: ${topChart.type} (${topChart.reason})`);
    }

    return parts.join(" | ");
  }
}

/**
 * Factory function
 */
export function createQueryAnalyzer(table?: string): QueryResponseAnalyzer {
  return new QueryResponseAnalyzer(table);
}
