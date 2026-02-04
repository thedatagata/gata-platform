// components/charts/FreshChartsWrapper.tsx
import FunnelChart from "../../islands/charts/FunnelChart.tsx";
import AutoChart from "../../islands/charts/AutoChart.tsx";
import type { ChartConfig } from "../../utils/smarter/autovisualization_dashboard/chart-generator.ts";

interface FreshChartsWrapperProps {
  config: ChartConfig;
  loading?: boolean;
  height?: number;
}

export default function FreshChartsWrapper({ 
  config, 
  loading = false, 
  height = 400 
}: FreshChartsWrapperProps) {
  // Loading state
  if (loading) {
    return (
      <div
        class="bg-white rounded-lg shadow p-6 flex items-center justify-center"
        style={{ height: `${height}px` }}
      >
        <div class="text-center">
          <div class="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p class="text-gray-900">Loading chart...</p>
        </div>
      </div>
    );
  }

  // No data state
  if (!config || !config.data || config.data.length === 0) {
    return (
      <div
        class="bg-white rounded-lg shadow p-6 flex items-center justify-center"
        style={{ height: `${height}px` }}
      >
        <p class="text-gray-900">No data available</p>
      </div>
    );
  }

  // Handle funnel charts with dedicated island
  if (config.type === 'funnel') {
    const funnelColors = [
      'rgb(54, 162, 235)',   // Blue
      'rgb(75, 192, 192)',   // Green
      'rgb(255, 205, 86)',   // Yellow
      'rgb(255, 159, 64)',   // Orange
      'rgb(255, 99, 132)',   // Red
    ];

    const labels = config.data.map(row => {
      const val = row[config.xKey];
      return val === null || val === undefined ? '' : String(val);
    });

    return (
      <FunnelChart
        data={{
          labels,
          datasets: [{
            data: config.data.map(row => row[config.yKeys[0]]),
            backgroundColor: funnelColors.slice(0, config.data.length),
            borderColor: funnelColors.slice(0, config.data.length),
            borderWidth: 2,
          }],
        }}
        options={{
          plugins: {
            legend: {
              display: false,
            },
            tooltip: {
              enabled: config.config.showTooltip !== false,
            },
          },
        }}
        height={height}
      />
    );
  }

  // Handle heatmap (not yet supported - show message)
  if (config.type === 'heatmap') {
    return (
      <div class="bg-white rounded-lg shadow p-6">
        <h3 class="text-lg font-semibold text-gray-900 mb-4">{config.title}</h3>
        <div class="bg-yellow-50 border border-yellow-200 rounded p-4">
          <p class="text-yellow-800 text-sm">
            Heatmap visualization not yet supported with Chart.js. 
            Displaying as grouped bar chart instead.
          </p>
        </div>
        {/* Fallback to bar chart */}
        <AutoChart config={{ ...config, type: 'bar' }} height={height} />
      </div>
    );
  }

  // Use AutoChart island for all other chart types
  // Supports: bar, line, area, stacked-bar, biaxial-bar, area-fill-by-value
  return <AutoChart config={config} height={height} />;
}
