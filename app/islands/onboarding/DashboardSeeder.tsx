import { useState, useMemo, useEffect } from "preact/hooks";


import { getSemanticMetadata, getAllRegisteredTables } from "../../utils/smarter/dashboard_utils/semantic-config.ts";

export interface SeedingInfo {
  table: string;
  measures: string[];      // Multiple measures for the charts
  primaryMeasure: string;   // For PoP cards
  dimension: string;
  dateField: string;
  timeRange: string;
  filters: Array<{ field: string; value: string }>;
}

interface DashboardSeederProps {
  initialTableName: string;
  onComplete: (seeding: SeedingInfo) => void;
  onBack: () => void;
}

export default function DashboardSeeder({ initialTableName, onComplete, onBack }: DashboardSeederProps) {
  const [selectedTable, setSelectedTable] = useState<string>(initialTableName);
  const [selectedMeasures, setSelectedMeasures] = useState<string[]>([]);
  const [primaryMeasure, setPrimaryMeasure] = useState<string>("");
  const [selectedDimension, setSelectedDimension] = useState<string>("");
  const [dateField, setDateField] = useState<string>("");
  const [timeRange, setTimeRange] = useState<string>("30 days");
  const [filters, setFilters] = useState<Array<{ field: string; value: string }>>([]);

  const semanticConfig = useMemo(() => getSemanticMetadata(selectedTable), [selectedTable]);
  const availableTables = useMemo(() => getAllRegisteredTables(), []);

  // Reset selections when table changes
  useEffect(() => {
    setSelectedMeasures([]);
    setPrimaryMeasure("");
    setSelectedDimension("");
    setDateField("");
    setFilters([]);
  }, [selectedTable]);

  // Extract all measure aliases
  const measureOptions = useMemo(() => {
    const options: string[] = [];
    Object.values(semanticConfig.measures).forEach(m => {
      if (m.aggregations) {
        m.aggregations.forEach((agg: Record<string, unknown>) => {
          const type = Object.keys(agg)[0];
          const aggDetail = agg[type] as { alias: string };
          options.push(aggDetail.alias);
        });
      }
      if (m.formula) {
        Object.keys(m.formula).forEach(k => options.push(k));
      }
    });
    return options;
  }, [semanticConfig]);

  // Extract all dimension aliases
  const dimensionOptions = useMemo(() => {
    return Object.values(semanticConfig.dimensions).map(d => d.alias_name);
  }, [semanticConfig]);

  // Extract temporal fields
  const temporalOptions = useMemo(() => {
    return Object.entries(semanticConfig.fields)
      .filter(([_, f]) => f.data_type_category === 'temporal')
      .map(([name, _]) => name);
  }, [semanticConfig]);

  // Extract categorical fields for filters
  const categoricalOptions = useMemo(() => {
    return Object.entries(semanticConfig.fields)
      .filter(([_, f]) => f.data_type_category === 'categorical')
      .map(([name, _]) => name);
  }, [semanticConfig]);

  const handleToggleMeasure = (alias: string) => {
    if (selectedMeasures.includes(alias)) {
      setSelectedMeasures(selectedMeasures.filter(m => m !== alias));
      if (primaryMeasure === alias) setPrimaryMeasure("");
    } else {
      setSelectedMeasures([...selectedMeasures, alias]);
      if (!primaryMeasure) setPrimaryMeasure(alias);
    }
  };

  const handleFinish = () => {
    if (selectedMeasures.length === 0 || !selectedDimension || !dateField) {
      alert("Please select at least one measure, dimension, and date field.");
      return;
    }

    onComplete({
      table: selectedTable,
      measures: selectedMeasures,
      primaryMeasure: primaryMeasure || selectedMeasures[0],
      dimension: selectedDimension,
      dateField,
      timeRange,
      filters
    });
  };

  return (
    <div class="max-w-4xl mx-auto w-full bg-gata-dark border-2 border-gata-green rounded-3xl p-8 shadow-2xl space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-700">
      <div class="text-center space-y-2">
        <h2 class="text-4xl font-extrabold text-gata-cream italic tracking-tighter">
          CONFIGURE YOUR <span class="text-gata-green">INSIGHT ENGINE</span> 
        </h2>
        <p class="text-gata-cream/60">Tell the AI how to slice your data for the initial view.</p>
      </div>

      <div class="grid md:grid-cols-2 gap-8">
        {/* Left Column: Metrics & Dimensions */}
        <div class="space-y-6">
          <div class="space-y-3">
             <label class="block text-xs font-bold text-gata-green autocomplete-widest">1. Select Data Source</label>
             <select 
               value={selectedTable}
               onChange={(e) => setSelectedTable((e.target as HTMLSelectElement).value)}
               class="w-full bg-gata-darker border-2 border-gata-green/20 rounded-xl py-3 px-4 text-gata-cream font-bold focus:outline-none focus:border-gata-green transition-all"
             >
               {availableTables.map(t => (
                 <option key={t} value={t}>{t.toUpperCase()}</option>
               ))}
             </select>
          </div>

          <div class="space-y-3">
             <label class="block text-xs font-bold text-gata-green uppercase tracking-widest">2. Select Key Metrics</label>
             <div class="flex flex-wrap gap-2 max-h-[150px] overflow-y-auto p-2 bg-gata-darker rounded-xl border border-gata-green/10">
                {measureOptions.map(alias => (
                  <button
                    key={alias}
                    type="button"
                    onClick={() => handleToggleMeasure(alias)}
                    class={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
                      selectedMeasures.includes(alias) 
                        ? 'bg-gata-green text-gata-dark' 
                        : 'bg-gata-dark border border-gata-green/20 text-gata-cream/60 hover:border-gata-green/60'
                    }`}
                  >
                    {alias}
                  </button>
                ))}
             </div>
          </div>

          <div class="space-y-3">
             <label class="block text-xs font-bold text-gata-green uppercase tracking-widest">3. Group By (Dimension)</label>
             <select 
               value={selectedDimension}
               onChange={(e) => setSelectedDimension((e.target as HTMLSelectElement).value)}
               class="w-full bg-gata-darker border-2 border-gata-green/20 rounded-xl py-3 px-4 text-gata-cream focus:outline-none focus:border-gata-green transition-all"
             >
               <option value="">Select a Dimension...</option>
               {dimensionOptions.map(d => (
                 <option key={d} value={d}>{d}</option>
               ))}
             </select>
          </div>

          <div class="space-y-3">
             <label class="block text-xs font-bold text-gata-green uppercase tracking-widest">4. Primary Metric for PoP Cards</label>
             <select 
               value={primaryMeasure}
               onChange={(e) => setPrimaryMeasure((e.target as HTMLSelectElement).value)}
               class="w-full bg-gata-darker border-2 border-gata-green/20 rounded-xl py-3 px-4 text-gata-cream focus:outline-none focus:border-gata-green transition-all"
             >
               <option value="">Choose the main success metric...</option>
               {selectedMeasures.map(m => (
                 <option key={m} value={m}>{m}</option>
               ))}
             </select>
             <p class="text-[10px] text-gata-cream/40 italic">This will generate the big "Period over Period" numbers at the top.</p>
          </div>
        </div>

        {/* Right Column: Time & Filters */}
        <div class="space-y-6">
          <div class="space-y-3">
             <label class="block text-xs font-bold text-gata-green uppercase tracking-widest">5. Time Analysis Field</label>
             <select 
               value={dateField}
               onChange={(e) => setDateField((e.target as HTMLSelectElement).value)}
               class="w-full bg-gata-darker border-2 border-gata-green/20 rounded-xl py-3 px-4 text-gata-cream focus:outline-none focus:border-gata-green transition-all"
             >
               <option value="">Select Date/Time Column...</option>
               {temporalOptions.map(t => (
                 <option key={t} value={t}>{t}</option>
               ))}
             </select>
          </div>

          <div class="space-y-3">
             <label class="block text-xs font-bold text-gata-green uppercase tracking-widest">6. Lookback Period</label>
             <select 
               value={timeRange}
               onChange={(e) => setTimeRange((e.target as HTMLSelectElement).value)}
               class="w-full bg-gata-darker border-2 border-gata-green/20 rounded-xl py-3 px-4 text-gata-cream focus:outline-none focus:border-gata-green transition-all"
             >
               <option value="7 days">Last 7 Days</option>
               <option value="30 days">Last 30 Days</option>
               <option value="90 days">Last 90 Days</option>
               <option value="12 months">Last 12 Months</option>
             </select>
          </div>

          {/* Filters (Simplified) */}
          <div class="space-y-3">
             <label class="block text-xs font-bold text-gata-green uppercase tracking-widest">7. Apply Quick Filters</label>
             <div class="space-y-2">
                {filters.map((f, i) => (
                  <div key={`${f.field}-${f.value}-${i}`} class="flex gap-2 animate-in slide-in-from-left-2 duration-300">
                    <span class="flex-1 bg-gata-darker p-2 rounded text-xs text-gata-cream/80 border border-gata-green/10">
                      {f.field} = <span class="text-gata-green">'{f.value}'</span>
                    </span>
                    <button 
                      type="button"
                      onClick={() => setFilters(filters.filter((_, idx) => idx !== i))}
                      class="text-red-400 hover:text-red-300 px-2"
                    >
                      ×
                    </button>
                  </div>
                ))}
                
                <div class="flex gap-2">
                  <select 
                    id="new-filter-field"
                    class="flex-1 bg-gata-darker border border-gata-green/20 rounded-lg p-2 text-xs text-gata-cream outline-none"
                  >
                    <option value="">Field...</option>
                    {categoricalOptions.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                  <input 
                    id="new-filter-value"
                    placeholder="Value..."
                    class="flex-1 bg-gata-darker border border-gata-green/20 rounded-lg p-2 text-xs text-gata-cream outline-none"
                  />
                  <button 
                    type="button"
                    onClick={() => {
                      const f = (document.getElementById('new-filter-field') as HTMLSelectElement).value;
                      const v = (document.getElementById('new-filter-value') as HTMLInputElement).value;
                      if (f && v) {
                        setFilters([...filters, { field: f, value: v }]);
                        (document.getElementById('new-filter-value') as HTMLInputElement).value = "";
                      }
                    }}
                    class="bg-gata-green text-gata-dark p-2 rounded-lg font-bold text-sm"
                  >
                    +
                  </button>
                </div>
             </div>
          </div>
        </div>
      </div>

      {/* Footer Actions */}
      <div class="pt-8 border-t border-gata-green/20 flex justify-between items-center">
        <button 
          type="button"
          onClick={onBack}
          class="text-gata-cream/40 hover:text-gata-cream transition-colors text-sm font-medium"
        >
          ← Back to Choice
        </button>
        <button 
          type="button"
          onClick={handleFinish}
          class="px-10 py-4 bg-gata-green text-gata-dark font-black rounded-2xl hover:bg-gata-hover transition-all shadow-[0_0_20px_rgba(144,193,55,0.3)] transform hover:scale-105"
        >
          GENERATE DASHBOARD →
        </button>
      </div>
    </div>
  );
}
