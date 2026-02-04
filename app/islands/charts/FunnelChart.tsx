// islands/charts/FunnelChart.tsx
import { useEffect, useRef } from "preact/hooks";
import { Chart, registerables } from "chart.js";

interface FunnelChartProps {
  data: {
    labels: string[];
    datasets: Array<{
      label?: string;
      data: number[];
      backgroundColor?: string | string[];
      borderColor?: string | string[];
      borderWidth?: number;
    }>;
  };
  options?: any;
  height?: number;
}

export default function FunnelChart({ data, options = {}, height = 400 }: FunnelChartProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const chartRef = useRef<any>(null);

  useEffect(() => {
    if (!canvasRef.current) return;

    const loadChart = async () => {
      try {
        // Import funnel plugin
        const FunnelModule = await import("chartjs-chart-funnel");
        const FunnelController = FunnelModule.FunnelController;
        const TrapezoidElement = FunnelModule.TrapezoidElement;

        // Register Chart.js components
        Chart.register(...registerables);
        
        // Register funnel controller and element
        Chart.register(FunnelController, TrapezoidElement);

        // Destroy existing chart if it exists
        if (chartRef.current) {
          chartRef.current.destroy();
        }

        // Create new chart
        chartRef.current = new Chart(canvasRef.current, {
          type: 'funnel',
          data: data,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            ...options,
          },
        });
      } catch (error) {
        console.error("Error loading funnel chart:", error);
      }
    };

    loadChart();

    // Cleanup
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
      }
    };
  }, [data, options]);

  return (
    <div class="bg-white rounded-lg shadow p-6">
      <div style={{ height: `${height}px`, position: "relative" }}>
        <canvas ref={canvasRef}></canvas>
      </div>
    </div>
  );
}
