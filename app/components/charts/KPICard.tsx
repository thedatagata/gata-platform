// components/charts/KPICard.tsx
interface KPICardProps {
  title: string;
  value: number | bigint | Uint8Array;
  previousValue?: number | bigint | Uint8Array;
  changePercent?: number | bigint | Uint8Array;
  format?: "number" | "currency" | "percentage";
  decimals?: number;
  description?: string;
  loading?: boolean;
}

// Convert Uint8Array to Number (for DuckDB integer values)
function uint8ArrayToNumber(arr: Uint8Array): number {
  let result = 0;
  for (let i = 0; i < Math.min(8, arr.length); i++) {
    result += arr[i] * Math.pow(256, i);
  }
  return result;
}

function toNumber(value: number | bigint | Uint8Array | undefined): number {
  if (value === undefined || value === null) return 0;
  if (typeof value === 'bigint') return Number(value);
  if (value instanceof Uint8Array) return uint8ArrayToNumber(value);
  return value;
}

function formatCompactNumber(val: number, decimals: number = 2): string {
  const absVal = Math.abs(val);
  const sign = val < 0 ? '-' : '';
  
  if (absVal >= 1_000_000_000) {
    return sign + (absVal / 1_000_000_000).toFixed(decimals) + 'B';
  } else if (absVal >= 1_000_000) {
    return sign + (absVal / 1_000_000).toFixed(decimals) + 'M';
  } else if (absVal >= 1_000) {
    return sign + (absVal / 1_000).toFixed(decimals) + 'K';
  }
  return sign + absVal.toFixed(decimals);
}

export default function KPICard({
  title,
  value,
  previousValue,
  changePercent,
  format = "number",
  decimals = 2,
  description,
  loading = false,
}: KPICardProps) {
  // Convert to Number
  const numValue = toNumber(value);
  const numPreviousValue = toNumber(previousValue);
  const numChangePercent = toNumber(changePercent);

  const formatValue = (val: number, compact: boolean = true) => {
    if (format === "currency") {
      if (compact) {
        return '$' + formatCompactNumber(val, decimals);
      }
      return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
      }).format(val);
    } else if (format === "percentage") {
      return `${val.toFixed(decimals)}%`;
    } else {
      if (compact) {
        return formatCompactNumber(val, decimals);
      }
      return new Intl.NumberFormat("en-US", {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
      }).format(val);
    }
  };

  const hasComparison = (previousValue !== undefined && previousValue !== null) || 
                       (changePercent !== undefined && changePercent !== null);
  const change = numChangePercent || (numPreviousValue ? ((numValue - numPreviousValue) / numPreviousValue) * 100 : 0);
  const isPositive = change > 0;
  const isNegative = change < 0;

  return (
    <div class="bg-white rounded-lg shadow p-6 border border-gray-200">
      {loading ? (
        <div class="animate-pulse">
          <div class="h-4 bg-gray-200 rounded w-3/4 mb-4"></div>
          <div class="h-8 bg-gray-200 rounded w-1/2 mb-2"></div>
          <div class="h-3 bg-gray-200 rounded w-full"></div>
        </div>
      ) : (
        <>
          <div class="text-sm font-medium text-gray-900 mb-2">{title}</div>
          <div class="text-3xl font-bold text-gray-900 mb-2">
            {formatValue(numValue, true)}
          </div>
          
          {hasComparison && (
            <div class="flex items-center gap-2 mb-1">
              <span class={`text-sm font-medium ${
                isPositive ? 'text-green-600' : isNegative ? 'text-red-600' : 'text-gray-900'
              }`}>
                {isPositive ? '↑' : isNegative ? '↓' : '→'} {Math.abs(change).toFixed(1)}%
              </span>
              {previousValue !== undefined && previousValue !== null && (
                <span class="text-xs text-gray-700">
                  vs {formatValue(numPreviousValue, true)}
                </span>
              )}
            </div>
          )}
          
          {description && (
            <div class="text-xs text-gray-700">{description}</div>
          )}
        </>
      )}
    </div>
  );
}
