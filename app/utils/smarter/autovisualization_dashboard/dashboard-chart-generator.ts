// utils/smarter/autovisualization_dashboard/dashboard-chart-generator.ts
import type { ChartConfig } from "./chart-generator.ts";
import { createQueryValidator } from "../dashboard_utils/semantic-query-validator.ts";
import { getSemanticMetadata } from "../dashboard_utils/semantic-config.ts";

const COLOR_PALETTE = [
  '#8884d8', '#82ca9d', '#ffc658', '#ff7c7c',
  '#8dd1e1', '#a4de6c', '#d0ed57', '#ffa07a'
];

function formatLabel(text: string): string {
  return text
    .replace(/_/g, ' ')
    .replace(/\b\w/g, l => l.toUpperCase())
    .replace(/Ltv/g, 'LTV')
    .replace(/30d/g, '(30d)');
}

function uint8ArrayToNumber(arr: Uint8Array): number {
  let result = 0;
  for (let i = 0; i < Math.min(8, arr.length); i++) {
    result += arr[i] * Math.pow(256, i);
  }
  return result;
}

function sanitizeValue(value: any): any {
  if (typeof value === 'bigint') {
    return Number(value);
  } else if (value instanceof Uint8Array) {
    return uint8ArrayToNumber(value);
  } else if (value instanceof Date) {
    return value.toISOString().split('T')[0];
  }
  return value;
}

function sanitizeData(data: any[]): any[] {
  return data.map(row => {
    const newRow: any = {};
    for (const key in row) {
      newRow[key] = sanitizeValue(row[key]);
    }
    return newRow;
  });
}

export function generateDashboardChartConfig(
  query: any,
  data: any[]
): ChartConfig {
  console.log('generateDashboardChartConfig called with:', { query, data });
  
  if (!query) {
    throw new Error('Query is required');
  }
  
  if (!Array.isArray(data)) {
    throw new Error('Data must be an array');
  }

  const sanitizedData = sanitizeData(data);
  console.log('Sanitized data:', sanitizedData);

  const semanticConfig = getSemanticMetadata(query.table);
  
  if (!semanticConfig) {
    throw new Error(`Table ${query.table} not found in semantic config`);
  }

  // Use semantic-query-validator to analyze query
  const validator = createQueryValidator(query.table);
  const analysis = validator.analyzeForVisualization({
    table: query.table,
    dimensions: (query.dimensions || []).map((d: any) => ({
      alias: typeof d === 'string' ? d : d.alias,
      sourceField: typeof d === 'string' ? d : d.sourceField,
      transformation: null
    })),
    measures: (query.measures || []).map((m: any) => ({
      alias: typeof m === 'string' ? m : m.alias,
      sourceField: typeof m === 'string' ? m : m.sourceField,
      aggregation: null,
      formula: null,
      type: 'aggregation' as const
    })),
    filters: query.filters || [],
    sql: query.sql || ""
  });

  console.log('Chart detection result:', analysis);

  // Extract dimensions and measures
  const dimensions = (query.dimensions || []).map((d: any) => typeof d === 'string' ? d : d.alias);
  const measures = (query.measures || []).map((m: any) => typeof m === 'string' ? m : m.alias);

  // Handle multiple dimensions + single measure (grouped bar chart)
  let xKey: string;
  let yKeys: string[];
  let formattedData: any[];
  
  if (dimensions.length >= 2 && measures.length === 1) {
    // Pivot data: first dimension = X-axis, other dimensions = bar groups
    xKey = dimensions[0];
    const groupDimension = dimensions[1];
    const measure = measures[0];

    const groupValues = [...new Set(sanitizedData.map(row => row[groupDimension]))];
    yKeys = groupValues.map(v => String(v));
    
    // Pivot the data
    const pivoted = new Map<string, any>();
    sanitizedData.forEach(row => {
      const xValue = row[xKey];
      const groupValue = String(row[groupDimension]);
      const measureValue = row[measure];
      
      if (!pivoted.has(xValue)) {
        pivoted.set(xValue, { [xKey]: xValue });
      }
      pivoted.get(xValue)![groupValue] = measureValue;
    });
    
    formattedData = Array.from(pivoted.values());
  } else {
    // Standard case
    xKey = dimensions[0] || 'index';
    yKeys = measures || [];
    
    if (yKeys.length === 0) {
      throw new Error('No measures specified for chart');
    }
    
    formattedData = sanitizedData.map((row, idx) => ({
      ...row,
      index: idx
    }));
  }

  const format: any = {};
  
  yKeys.forEach(measure => {
    const measureConfig = semanticConfig.measures[measure];
    if (measureConfig) {
      // Check aggregations
      if (measureConfig.aggregations) {
        measureConfig.aggregations.forEach((agg: any) => {
          const aggType = Object.keys(agg)[0];
          const aggConfig = agg[aggType];
          if (aggConfig.alias === measure) {
            format[measure] = {
              type: aggConfig.format,
              decimals: aggConfig.decimals || 0
            };
          }
        });
      }
      // Check formulas
      if (measureConfig.formula && measureConfig.formula[measure]) {
        format[measure] = {
          type: measureConfig.formula[measure].format,
          decimals: measureConfig.formula[measure].decimals || 0
        };
      }
    }
  });

  const isTimeSeries = dimensions[0]?.includes('date') || 
                       dimensions[0]?.includes('time');

  const title = query.title || generateTitle(yKeys, dimensions);

  const chartType = analysis.recommendedCharts[0]?.type || 'bar';

  const config: ChartConfig = {
    type: chartType as ChartConfig['type'],
    title,
    xKey,
    yKeys,
    data: formattedData,
    config: {
      colors: COLOR_PALETTE.slice(0, yKeys.length),
      stacked: analysis.queryType === 'breakdown',
      showLegend: yKeys.length > 1,
      showGrid: true,
      showTooltip: true,
      orientation: 'vertical',
      isTimeSeries,
      format
    }
  };

  console.log('Generated RechartsConfig:', config);
  
  return config;
}

function generateTitle(measures: string[], dimensions?: string[]): string {
  const measuresLabel = measures.map(m => formatLabel(m)).join(' & ');
  const dimensionsLabel = dimensions?.map(d => formatLabel(d)).join(' by ') || '';
  
  if (dimensionsLabel) {
    return `${measuresLabel} by ${dimensionsLabel}`;
  }
  return measuresLabel;
}
