// islands/dashboard/observability/PipelineHealthDashboard.tsx
import { useState, useEffect } from "preact/hooks";
import {
  createPlatformAPIClient,
  type ObservabilitySummary,
  type RunResult,
  type IdentityResolutionStats,
} from "../../../utils/api/platform-api-client.ts";

interface PipelineHealthDashboardProps {
  tenantSlug: string;
  onBack: () => void;
}

function formatRelativeTime(dateStr: string | null): string {
  if (!dateStr) return "Never";
  const date = new Date(dateStr);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  if (diffMins < 1) return "Just now";
  if (diffMins < 60) return `${diffMins}m ago`;
  const diffHrs = Math.floor(diffMins / 60);
  if (diffHrs < 24) return `${diffHrs}h ago`;
  const diffDays = Math.floor(diffHrs / 24);
  return `${diffDays}d ago`;
}

function statusColor(status: string): string {
  switch (status.toLowerCase()) {
    case "success":
    case "pass":
      return "text-gata-green";
    case "fail":
    case "error":
      return "text-red-400";
    case "skipped":
      return "text-gata-cream/30";
    default:
      return "text-gata-cream/60";
  }
}

export default function PipelineHealthDashboard({ tenantSlug, onBack }: PipelineHealthDashboardProps) {
  const [summary, setSummary] = useState<ObservabilitySummary | null>(null);
  const [runs, setRuns] = useState<RunResult[]>([]);
  const [identity, setIdentity] = useState<IdentityResolutionStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    const client = createPlatformAPIClient();

    try {
      const [summaryData, runsData, identityData] = await Promise.allSettled([
        client.getObservabilitySummary(tenantSlug),
        client.getRunResults(tenantSlug, 20),
        client.getIdentityResolution(tenantSlug),
      ]);

      if (summaryData.status === "fulfilled") setSummary(summaryData.value);
      if (runsData.status === "fulfilled") setRuns(runsData.value);
      if (identityData.status === "fulfilled") setIdentity(identityData.value);

      if (summaryData.status === "rejected" && runsData.status === "rejected") {
        setError("Failed to load observability data. Is the platform API running?");
      }
    } catch (err) {
      console.error("Observability fetch error:", err);
      setError("Failed to connect to the platform API.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [tenantSlug]);

  if (loading) {
    return (
      <div class="min-h-screen bg-gata-dark flex items-center justify-center">
        <div class="text-center space-y-6">
          <div class="w-16 h-16 border-4 border-gata-green/30 border-t-gata-green rounded-full animate-spin mx-auto" />
          <p class="text-gata-green font-mono text-sm uppercase tracking-widest">Loading Pipeline Health...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div class="min-h-screen bg-gata-dark flex items-center justify-center p-4">
        <div class="max-w-md text-center space-y-6">
          <div class="w-16 h-16 bg-red-400/10 rounded-2xl flex items-center justify-center mx-auto">
            <svg class="w-8 h-8 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
            </svg>
          </div>
          <p class="text-red-400 font-mono text-sm">{error}</p>
          <div class="flex gap-4 justify-center">
            <button type="button" onClick={fetchData} class="px-6 py-3 bg-gata-green text-gata-dark rounded-xl text-xs font-black uppercase tracking-widest hover:bg-gata-hover transition-colors">
              Retry
            </button>
            <button type="button" onClick={onBack} class="px-6 py-3 border border-gata-green/30 text-gata-cream/60 rounded-xl text-xs font-black uppercase tracking-widest hover:text-gata-green transition-colors">
              Back
            </button>
          </div>
        </div>
      </div>
    );
  }

  const passRate = summary
    ? ((summary.pass_count / Math.max(summary.pass_count + summary.fail_count + summary.error_count, 1)) * 100)
    : 0;

  return (
    <div class="min-h-screen bg-gata-dark p-6 md:p-10">
      <div class="max-w-6xl mx-auto space-y-8">
        {/* Header */}
        <div class="flex items-center justify-between">
          <div>
            <button type="button" onClick={onBack} class="text-gata-green hover:text-gata-hover font-bold text-xs uppercase tracking-widest mb-2 flex items-center gap-2 transition-colors">
              ← Back to Models
            </button>
            <h1 class="text-4xl font-black text-gata-cream italic tracking-tighter uppercase">
              Pipeline Health
            </h1>
            <p class="text-xs text-gata-cream/40 font-medium uppercase tracking-[0.2em] mt-1">{tenantSlug}</p>
          </div>
          <button type="button" onClick={fetchData} class="px-4 py-2 border border-gata-green/20 text-gata-cream/40 rounded-lg text-xs font-bold uppercase tracking-widest hover:text-gata-green hover:border-gata-green/50 transition-all">
            Refresh
          </button>
        </div>

        {/* Summary KPI Cards */}
        {summary && (
          <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div class="bg-gata-dark/60 border border-gata-green/15 rounded-2xl p-6">
              <p class="text-[10px] font-black text-gata-cream/30 uppercase tracking-widest mb-2">Total Models</p>
              <p class="text-3xl font-black text-gata-cream">{summary.models_count}</p>
            </div>
            <div class="bg-gata-dark/60 border border-gata-green/15 rounded-2xl p-6">
              <p class="text-[10px] font-black text-gata-cream/30 uppercase tracking-widest mb-2">Last Run</p>
              <p class="text-3xl font-black text-gata-cream">{formatRelativeTime(summary.last_run_at)}</p>
            </div>
            <div class="bg-gata-dark/60 border border-gata-green/15 rounded-2xl p-6">
              <p class="text-[10px] font-black text-gata-cream/30 uppercase tracking-widest mb-2">Pass Rate</p>
              <p class="text-3xl font-black text-gata-green">{passRate.toFixed(1)}%</p>
              <div class="mt-2 h-1.5 bg-gata-dark rounded-full overflow-hidden">
                <div class="h-full bg-gata-green rounded-full transition-all" style={{ width: `${passRate}%` }} />
              </div>
            </div>
            <div class="bg-gata-dark/60 border border-gata-green/15 rounded-2xl p-6">
              <p class="text-[10px] font-black text-gata-cream/30 uppercase tracking-widest mb-2">Avg Execution</p>
              <p class="text-3xl font-black text-gata-cream">{summary.avg_execution_time.toFixed(2)}s</p>
            </div>
          </div>
        )}

        {/* Identity Resolution Card */}
        {identity && (
          <div class="bg-gata-dark/60 border border-gata-green/15 rounded-2xl p-6">
            <h3 class="text-sm font-black text-gata-cream uppercase tracking-widest mb-6">Identity Resolution</h3>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-6">
              <div>
                <p class="text-[10px] font-black text-gata-cream/30 uppercase tracking-widest mb-1">Total Users</p>
                <p class="text-2xl font-black text-gata-cream">{identity.total_users.toLocaleString()}</p>
              </div>
              <div>
                <p class="text-[10px] font-black text-gata-cream/30 uppercase tracking-widest mb-1">Resolved</p>
                <p class="text-2xl font-black text-gata-green">{identity.resolved_customers.toLocaleString()}</p>
              </div>
              <div>
                <p class="text-[10px] font-black text-gata-cream/30 uppercase tracking-widest mb-1">Anonymous</p>
                <p class="text-2xl font-black text-gata-cream/60">{identity.anonymous_users.toLocaleString()}</p>
              </div>
              <div>
                <p class="text-[10px] font-black text-gata-cream/30 uppercase tracking-widest mb-1">Resolution Rate</p>
                <p class="text-2xl font-black text-gata-green">{(identity.resolution_rate * 100).toFixed(1)}%</p>
              </div>
            </div>
            <div class="mt-4 h-2 bg-gata-dark rounded-full overflow-hidden">
              <div class="h-full bg-gata-green rounded-full transition-all" style={{ width: `${identity.resolution_rate * 100}%` }} />
            </div>
            <div class="flex justify-between mt-2 text-[10px] text-gata-cream/30 uppercase tracking-widest">
              <span>{identity.total_events.toLocaleString()} events</span>
              <span>{identity.total_sessions.toLocaleString()} sessions</span>
            </div>
          </div>
        )}

        {/* Run Results Table */}
        {runs.length > 0 && (
          <div class="bg-gata-dark/60 border border-gata-green/15 rounded-2xl p-6">
            <h3 class="text-sm font-black text-gata-cream uppercase tracking-widest mb-6">Recent Run Results</h3>
            <div class="overflow-x-auto">
              <table class="w-full text-left">
                <thead>
                  <tr class="border-b border-gata-green/10">
                    <th class="pb-3 text-[10px] font-black text-gata-cream/30 uppercase tracking-widest">Model Name</th>
                    <th class="pb-3 text-[10px] font-black text-gata-cream/30 uppercase tracking-widest">Status</th>
                    <th class="pb-3 text-[10px] font-black text-gata-cream/30 uppercase tracking-widest text-right">Rows</th>
                    <th class="pb-3 text-[10px] font-black text-gata-cream/30 uppercase tracking-widest text-right">Time</th>
                    <th class="pb-3 text-[10px] font-black text-gata-cream/30 uppercase tracking-widest text-right">Run At</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((run, i) => (
                    <tr key={i} class="border-b border-gata-green/5 hover:bg-gata-green/5 transition-colors">
                      <td class="py-3 text-xs font-mono text-gata-cream/80">{run.model_name}</td>
                      <td class={`py-3 text-xs font-black uppercase tracking-widest ${statusColor(run.status)}`}>{run.status}</td>
                      <td class="py-3 text-xs font-mono text-gata-cream/60 text-right">{run.rows_affected ?? '-'}</td>
                      <td class="py-3 text-xs font-mono text-gata-cream/60 text-right">{run.execution_time_seconds.toFixed(3)}s</td>
                      <td class="py-3 text-xs font-mono text-gata-cream/40 text-right">{formatRelativeTime(run.run_started_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
