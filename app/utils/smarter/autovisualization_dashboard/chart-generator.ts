// utils/smarter/autovisualization_dashboard/chart-generator.ts
import { createQueryValidator } from "../dashboard_utils/semantic-query-validator.ts";
import type { QueryResponse } from "./webllm-handler.ts";

/**
 * Chart.js compatible config for AutoChart.tsx
 */
export interface ChartConfig {
  type: "bar" | "line" | "area" | "stacked-bar" | "biaxial-bar" | "area-fill-by-value";
  title: string;
  xKey: string;
  yKeys: string[];
  data: any[];
  config: {
    colors?: string[];
    stacked?: boolean;
    showLegend?: boolean;
    showGrid?: boolean;
    showTooltip?: boolean;
    orientation?: 'horizontal' | 'vertical';
    isTimeSeries?: boolean;
    format?: Record<string, { type: 'currency' | 'percentage' | 'number'; decimals?: number }>;
  };
}

export interface ChartGenerationResult {
  chartConfig: ChartConfig;
  queryType: string;
  alternativeCharts: ChartConfig[];
}

const COLOR_PALETTE = [
  'rgb(54, 162, 235)',   // Blue
  'rgb(75, 192, 192)',   // Green  
  'rgb(255, 205, 86)',   // Yellow
  'rgb(255, 99, 132)',   // Red
  'rgb(153, 102, 255)',  // Purple
  'rgb(255, 159, 64)',   // Orange
];

/**
 * Generate Chart.js config from query response
 */
export function generateChartFromAnalysis(
  queryResponse: QueryResponse,
  data: any[]
): ChartGenerationResult {
  const validator = createQueryValidator(queryResponse.table);
  
  // Analyze query for visualization
  const analysis = validator.analyzeForVisualization({
    table: queryResponse.table,
    dimensions: queryResponse.dimensions.map(d => ({ ...d, transformation: null })),
    measures: queryResponse.measures.map(m => ({ ...m, aggregation: null, formula: null, type: 'aggregation' as const })),
    filters: queryResponse.filters || [],
    sql: queryResponse.sql
  });

  console.log("📊 [ChartGenerator] Analysis:", analysis);

  const primaryChart = buildChartConfig(
    analysis.recommendedCharts[0],
    queryResponse,
    data,
    analysis
  );

  const alternativeCharts = analysis.recommendedCharts.slice(1).map(rec =>
    buildChartConfig(rec, queryResponse, data, analysis)
  );

  return {
    chartConfig: primaryChart,
    queryType: analysis.queryType,
    alternativeCharts
  };
}

function buildChartConfig(
  recommendation: { type: string; reason: string },
  queryResponse: QueryResponse,
  data: any[],
  analysis: any
): ChartConfig {
  const dims = queryResponse.dimensions.map(d => d.alias);
  const measures = queryResponse.measures.map(m => m.alias);
  
  // Determine chart type and structure
  const xKey = dims[0] || 'metric';
  const yKeys = measures;
  
  // Build format configs from measure metadata
  const format: Record<string, any> = {};
  measures.forEach(m => {
    if (m.includes('rate') || m.includes('percentage')) {
      format[m] = { type: 'percentage', decimals: 1 };
    } else if (m.includes('revenue')) {
      format[m] = { type: 'currency', decimals: 2 };
    } else {
      format[m] = { type: 'number', decimals: 0 };
    }
  });

  const isTimeSeries = analysis.dimensionCategories.temporal.length > 0;
  
  // Map recommendation type to Chart.js types
  let chartType: ChartConfig['type'] = 'bar';
  if (recommendation.type === 'line') chartType = 'line';
  else if (recommendation.type === 'area') chartType = 'area';
  else if (analysis.queryType === 'breakdown') chartType = 'stacked-bar';

  const title = dims.length > 0
    ? `${formatLabel(measures[0])} by ${formatLabel(dims[0])}`
    : `${measures.map(formatLabel).join(' vs ')}`;

  return {
    type: chartType,
    title,
    xKey,
    yKeys,
    data,
    config: {
      colors: COLOR_PALETTE.slice(0, yKeys.length),
      stacked: chartType === 'stacked-bar',
      showLegend: yKeys.length > 1,
      showGrid: true,
      showTooltip: true,
      orientation: 'vertical',
      isTimeSeries,
      format
    }
  };
}

function formatLabel(text: string): string {
  return text
    .split("_")
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}
