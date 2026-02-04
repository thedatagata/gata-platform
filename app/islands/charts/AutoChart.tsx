// islands/charts/AutoChart.tsx
import { useEffect, useRef } from "preact/hooks";
import { Chart, registerables } from "chart.js";
import type { ChartConfig } from "../../utils/smarter/autovisualization_dashboard/chart-generator.ts";

interface AutoChartProps {
  config: ChartConfig;
  height?: number;
}

// Color palette matching existing implementation
const COLOR_MAP: Record<number, string> = {
  0: 'rgb(54, 162, 235)',   // Blue
  1: 'rgb(75, 192, 192)',   // Green
  2: 'rgb(255, 205, 86)',   // Yellow
  3: 'rgb(255, 99, 132)',   // Red
  4: 'rgb(153, 102, 255)',  // Purple
  5: 'rgb(255, 159, 64)',   // Orange
  6: 'rgb(201, 203, 207)',  // Grey
};

function transparentize(color: string, alpha: number): string {
  const rgb = color.match(/\d+/g);
  if (!rgb || rgb.length < 3) return color;
  return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, ${alpha})`;
}

export default function AutoChart({ config, height = 400 }: AutoChartProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const chartInstance = useRef<Chart | null>(null);

  useEffect(() => {
    if (!canvasRef.current) return;

    // Register Chart.js components once
    Chart.register(...registerables);

    // Destroy existing chart if it exists
    if (chartInstance.current) {
      chartInstance.current.destroy();
    }

    // Prepare data for Chart.js
    const labels = config.data.map(row => {
      const val = row[config.xKey];
      return val === null || val === undefined ? '' : String(val);
    });

    const datasets = config.yKeys.map((yKey, idx) => {
      const color = COLOR_MAP[idx] || COLOR_MAP[idx % Object.keys(COLOR_MAP).length];
      
      return {
        label: yKey.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
        data: config.data.map(row => row[yKey]),
        borderColor: color,
        backgroundColor: config.type === 'area' || config.type === 'area-fill-by-value'
          ? transparentize(color, 0.5)
          : transparentize(color, 0.7),
        borderWidth: 2,
        fill: config.type === 'area' || config.type === 'area-fill-by-value',
        yAxisID: config.type === 'biaxial-bar' ? (idx === 0 ? 'y' : 'y1') : 'y',
      };
    });

    // Determine chart type
    let chartType: 'bar' | 'line' = 'bar';
    if (config.type === 'line' || config.type === 'area' || config.type === 'area-fill-by-value') {
      chartType = 'line';
    }

    // Build chart options
    const options: any = {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      scales: {
        y: {
          beginAtZero: true,
          stacked: config.type === 'stacked-bar' || config.config.stacked,
          position: 'left',
        },
        x: {
          stacked: config.type === 'stacked-bar' || config.config.stacked,
        },
      },
      plugins: {
        legend: {
          display: config.config.showLegend !== false,
        },
        tooltip: {
          enabled: config.config.showTooltip !== false,
          callbacks: {
            label: function(context: any) {
              let label = context.dataset.label || '';
              if (label) {
                label += ': ';
              }
              if (context.parsed.y !== null) {
                const format = config.config.format?.[context.dataset.label];
                if (format) {
                  label += formatValue(context.parsed.y, format);
                } else {
                  label += context.parsed.y.toLocaleString();
                }
              }
              return label;
            }
          }
        },
      },
    };

    // Add second Y-axis for biaxial charts
    if (config.type === 'biaxial-bar') {
      options.scales.y1 = {
        beginAtZero: true,
        position: 'right',
        grid: {
          drawOnChartArea: false,
        },
      };
    }

    // Handle area-fill-by-value (positive/negative coloring)
    if (config.type === 'area-fill-by-value' && datasets.length > 0) {
      datasets[0].backgroundColor = (context: any) => {
        const value = context.raw;
        if (value === null || value === undefined) return 'rgba(128, 128, 128, 0.3)';
        return value >= 0
          ? 'rgba(34, 197, 94, 0.3)'  // Green for positive
          : 'rgba(239, 68, 68, 0.3)';  // Red for negative
      };
      datasets[0].borderColor = (context: any) => {
        const value = context.raw;
        if (value === null || value === undefined) return 'rgba(128, 128, 128, 1)';
        return value >= 0
          ? 'rgba(34, 197, 94, 1)'
          : 'rgba(239, 68, 68, 1)';
      };
    }

    // Create the chart
    chartInstance.current = new Chart(canvasRef.current, {
      type: chartType,
      data: { labels, datasets },
      options,
    });

    // Cleanup on unmount
    return () => {
      if (chartInstance.current) {
        chartInstance.current.destroy();
        chartInstance.current = null;
      }
    };
  }, [config]);

  return (
    <div class="bg-white rounded-lg shadow p-6">
      <h3 class="text-lg font-semibold text-gray-900 mb-4">{config.title}</h3>
      <div style={{ height: `${height}px`, position: 'relative' }}>
        <canvas ref={canvasRef}></canvas>
      </div>
    </div>
  );
}

function formatValue(
  value: number,
  format: { type: 'currency' | 'percentage' | 'number'; decimals?: number }
): string {
  const decimals = format.decimals ?? 0;
  
  switch (format.type) {
    case 'currency':
      return `$${value.toLocaleString(undefined, {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
      })}`;
    case 'percentage':
      return `${value.toFixed(decimals)}%`;
    case 'number':
    default:
      return value.toLocaleString(undefined, {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
      });
  }
}
