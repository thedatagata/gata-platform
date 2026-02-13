// islands/app_utils/SemanticProfiler.tsx
import { useState, useEffect } from "preact/hooks";
import { profileTable, SemanticLayer, SemanticField, getDefaultDimensionsForField, getDefaultMeasuresForField, SemanticFormula } from "../../utils/system/semantic-profiler.ts";
import { createPlatformAPIClient } from "../../utils/api/platform-api-client.ts";

interface SemanticProfilerProps {
  db: unknown;
  tableName: string;
  webllmEngine?: unknown; // WebLLMSemanticHandler
  onComplete: (config: SemanticLayer) => void;
  onCancel: () => void;
  tenantSlug?: string;
}

interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
}

export default function SemanticProfiler({ db, tableName, webllmEngine, onComplete, onCancel, tenantSlug }: SemanticProfilerProps) {
  const [loading, setLoading] = useState(true);
  const [config, setConfig] = useState<SemanticLayer | null>(null);
  
  // UI State
  const [activeTab, setActiveTab] = useState<'setup' | 'fields' | 'dimensions' | 'measures' | 'preview'>('preview');
  
  // Split Pane State (for Preview tab)
  const [splitRatio, _setSplitRatio] = useState(0.6); // 60% Left (Preview), 40% Right (Chat)
  
  // Chat State
  const [chatHistory, setChatHistory] = useState<ChatMessage[]>([]);
  const [userQuery, setUserQuery] = useState("");
  const [isChatting, setIsChatting] = useState(false);

  // Preview Data
  const [previewRows, setPreviewRows] = useState<any[]>([]);
  
  // Editor State (Old)
  const [glossary, setGlossary] = useState("");
  const [_aiLoading, setAiLoading] = useState(false);
  const [showAddModal, setShowAddModal] = useState<'dimension' | 'measure' | null>(null);
  const [newName, setNewName] = useState("");
  const [newAlias, setNewAlias] = useState("");
  const [newSQL, setNewSQL] = useState("");

  useEffect(() => {
    const initProfile = async () => {
      try {
        setLoading(true);
        // 1. Fetch live preview from backend if tenantSlug matches
        let rows: any[] = [];
        if (tenantSlug) {
             const client = createPlatformAPIClient();
             try {
                const res = await client.executeQuery(tenantSlug, {
                    model: tableName,
                    dimensions: [],
                    measures: [],
                    calculated_measures: [],
                    filters: [],
                    joins: [],
                    order_by: [],
                    limit: 100
                });
                rows = res.data;
             } catch (e) {
                 console.warn("Failed to fetch backend preview, falling back to local query if available", e);
             }
        }
        
        // 2. Profile the table (Schema inference)
        const draft = await profileTable(db as { query: (s: string) => Promise<unknown> }, tableName);
        setConfig(draft);
        
        const initialGlossary = Object.entries(draft.fields).map(([name, f]) => `${name}: ${f.description}`).join('\n');
        setGlossary(initialGlossary);
        
        if (rows.length > 0) {
            setPreviewRows(rows);
            // Insert into local DuckDB for WebLLM to query (Optional/TODO)
            // For now, we rely on WebLLM having context or just chat logic
        }

      } catch (err) {
        console.error("Profiling failed:", err);
      } finally {
        setLoading(false);
      }
    };
    initProfile();
  }, [db, tableName, tenantSlug]);

  // --- Helper Functions (Editor) ---

  const applyGlossary = (text: string) => {
    if (!config) return;
    const newFields = { ...config.fields };
    text.split('\n').forEach(line => {
      const [col, ...descParts] = line.split(':');
      if (descParts.length > 0) {
        const colName = col.trim();
        const desc = descParts.join(':').trim();
        if (newFields[colName]) newFields[colName] = { ...newFields[colName], description: desc };
      }
    });
    setConfig({ ...config, fields: newFields });
  };

  const generateAiDescriptions = async () => {
    if (!webllmEngine || !config) return;
    setAiLoading(true);
    try {
      const colNames = Object.keys(config.fields).join(', ');
      const prompt = `Generate brief descriptions for columns in table "${tableName}": ${colNames}. Format as col: description. Only return mappings.`;
      const response = await (webllmEngine as { chat: (p: string) => Promise<string> }).chat(prompt);
      setGlossary(response);
      applyGlossary(response);
    } catch (e) {
      console.error(e);
    } finally {
      setAiLoading(false);
    }
  };

  const updateField = (name: string, updates: Partial<SemanticField>) => {
    if (!config) return;
    const newFields = { ...config.fields, [name]: { ...config.fields[name], ...updates } };
    
    // If category changed, sync dimensions and measures
    if (updates.data_type_category) {
      const category = updates.data_type_category;
      const newDimensions = { ...config.dimensions };
      const newMeasures = { ...config.measures };
      
      delete newDimensions[name];
      delete newMeasures[name];
      
      const fieldDimensions = getDefaultDimensionsForField(name, category);
      const fieldMeasures = getDefaultMeasuresForField(name, category);
      
      Object.assign(newDimensions, fieldDimensions);
      Object.assign(newMeasures, fieldMeasures);
      
      setConfig({ ...config, fields: newFields, dimensions: newDimensions, measures: newMeasures });
    } else {
      setConfig({ ...config, fields: newFields });
    }
  };

  const handleAddCustom = () => {
    if (!config || !newAlias || !newSQL) return;
    const updated = { ...config };
    if (showAddModal === 'dimension') {
      updated.dimensions[newName || newAlias] = {
        alias_name: newAlias,
        transformation: newSQL 
      };
    } else {
      const targetBase = newName || 'custom_calc';
      if (!updated.measures[targetBase]) updated.measures[targetBase] = { aggregations: [], formula: {} };
      updated.measures[targetBase].formula = {
        ...updated.measures[targetBase].formula,
        [newAlias]: { sql: newSQL, description: "Custom formula measure" }
      };
    }
    setConfig(updated);
    setShowAddModal(null);
    setNewName(""); setNewAlias(""); setNewSQL("");
  };

  const handleChatParams = async () => {
      if (!userQuery.trim() || !webllmEngine) return;
      const q = userQuery;
      setUserQuery("");
      setChatHistory(prev => [...prev, { role: 'user', content: q }]);
      setIsChatting(true);

      try {
          const response = await (webllmEngine as any).chat(q);
          setChatHistory(prev => [...prev, { role: 'assistant', content: response }]);
      } catch (e) {
          setChatHistory(prev => [...prev, { role: 'assistant', content: "Error processing query." }]);
      } finally {
          setIsChatting(false);
      }
  };

  if (loading) return <div class="p-20 text-center text-gata-green animate-pulse font-mono tracking-widest">LOADING PREVIEW...</div>;
  if (!config) return <div class="p-20 text-center text-red-500">Failed to load schema.</div>;

  return (
    <div class="flex flex-col h-[85vh] bg-gata-dark border-2 border-gata-green/40 rounded-[2rem] overflow-hidden shadow-[0_0_50px_rgba(0,0,0,0.5)]">
      {/* Header */}
      <div class="p-8 border-b border-gata-green/10 bg-gata-darker/50 flex justify-between items-center">
        <div>
          <h2 class="text-3xl font-black text-gata-cream italic tracking-tighter">SEMANTIC <span class="text-gata-green">PROFILER</span></h2>
          <p class="text-xs text-gata-cream/40 font-bold uppercase tracking-widest mt-1">Refining: <span class="text-gata-green">{tableName}</span></p>
        </div>
        <div class="flex gap-4">
          <button type="button" onClick={onCancel} class="text-gata-cream/40 hover:text-gata-cream font-bold text-xs uppercase tracking-widest transition-colors">Discard</button>
          <button type="button" onClick={() => onComplete(config)} class="px-8 py-3 bg-gata-green text-gata-dark font-black rounded-xl hover:bg-gata-hover transition-all shadow-lg">CREATE MODEL →</button>
        </div>
      </div>

      {/* Tabs */}
      <nav class="flex px-8 bg-gata-darker/30 border-b border-gata-green/5">
        {(['setup', 'fields', 'dimensions', 'measures', 'preview'] as const).map(tab => (
          <button type="button" key={tab} onClick={() => setActiveTab(tab)} class={`px-6 py-4 text-xs font-black uppercase tracking-widest transition-all border-b-4 ${activeTab === tab ? 'border-gata-green text-gata-green' : 'border-transparent text-gata-cream/30 hover:text-gata-cream'}`}>{tab}</button>
        ))}
      </nav>

      {/* Content */}
      <div class="flex-1 overflow-y-auto bg-gradient-to-b from-gata-darker/20 to-transparent"> 
        {/* Note: Removed padding from parent to allow split pane to take full width in preview */}
        
        {activeTab === 'setup' && (
          <div class="p-8 max-w-4xl space-y-8">
            <div class="space-y-3">
              <label class="block text-xs font-black text-gata-green uppercase tracking-widest">Dataset Description</label>
              <textarea value={config.description} onChange={(e) => setConfig({...config, description: (e.target as HTMLTextAreaElement).value})} class="w-full bg-gata-darker border-2 border-gata-green/10 rounded-2xl p-4 text-gata-cream focus:border-gata-green outline-none min-h-[120px]" />
            </div>
            <div class="space-y-3">
              <div class="flex justify-between items-center">
                <label class="block text-xs font-black text-gata-green uppercase tracking-widest">Glossary & Descriptions</label>
                {webllmEngine && <button type="button" onClick={generateAiDescriptions} class="text-[10px] bg-gata-green/10 text-gata-green border border-gata-green/20 px-3 py-1 rounded-full font-bold hover:bg-gata-green/20"> AI AUTOFILL</button>}
              </div>
              <textarea value={glossary} onChange={(e) => { const v = (e.target as HTMLTextAreaElement).value; setGlossary(v); applyGlossary(v); }} class="w-full bg-gata-darker border-2 border-gata-green/10 rounded-2xl p-4 font-mono text-sm text-gata-green/80 focus:border-gata-green outline-none min-h-[300px]" />
            </div>
          </div>
        )}

        {activeTab === 'fields' && (
          <div class="p-8 overflow-x-auto rounded-2xl border border-gata-green/10 bg-gata-darker/30">
            <table class="w-full text-left">
              <thead>
                <tr class="text-[10px] font-black text-gata-green uppercase tracking-[0.2em] border-b border-gata-green/10">
                  <th class="p-6">Field Name</th>
                  <th class="p-6">Data Type</th>
                  <th class="p-6">Analytical Category</th>
                  <th class="p-6">Human Description</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-gata-green/5">
                {Object.entries(config.fields).map(([name, field]) => (
                  <tr key={name} class="group hover:bg-gata-green/5 transition-colors">
                    <td class="p-6 font-mono text-sm text-gata-green">{name}</td>
                    <td class="p-6 text-xs text-gata-cream/40 uppercase">{field.md_data_type}</td>
                    <td class="p-6">
                      <select value={field.data_type_category} onChange={(e) => updateField(name, { data_type_category: (e.target as HTMLSelectElement).value as SemanticField['data_type_category'] })} class="bg-gata-dark border border-gata-green/20 rounded-lg px-3 py-2 text-xs text-gata-cream outline-none focus:border-gata-green transition-all">
                        {['categorical', 'numerical', 'temporal', 'identifier', 'continuous'].map(c => <option key={c} value={c}>{c}</option>)}
                      </select>
                    </td>
                    <td class="p-6">
                      <input type="text" value={field.description} onChange={(e) => updateField(name, { description: (e.target as HTMLInputElement).value })} class="w-full bg-transparent border-b border-gata-green/10 focus:border-gata-green outline-none text-sm py-1 transition-all" />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {activeTab === 'dimensions' && (
          <div class="p-8 space-y-8">
            <div class="flex justify-between items-center">
              <p class="text-sm text-gata-cream/40 italic">Dimensions define how you slice and dice your metrics.</p>
              <button type="button" onClick={() => setShowAddModal('dimension')} class="bg-gata-green text-gata-dark px-4 py-2 rounded-lg font-black text-[10px] uppercase tracking-widest hover:bg-gata-hover">+ Add Calculated Dimension</button>
            </div>
            <div class="grid md:grid-cols-3 gap-6">
              {Object.entries(config.dimensions).map(([name, dim]) => (
                <div key={name} class="p-6 bg-gata-darker border-2 border-gata-green/10 rounded-2xl hover:border-gata-green/40 transition-all group">
                   <div class="flex justify-between items-center mb-4">
                      <span class="text-[10px] font-black text-gata-green/60 uppercase tracking-widest font-mono truncate mr-4">{name}</span>
                      <button type="button" onClick={() => { const c = {...config!}; delete c.dimensions[name]; setConfig(c); }} class="text-red-400 opacity-0 group-hover:opacity-100 transition-opacity">×</button>
                   </div>
                   <h4 class="text-xl font-black text-gata-cream italic group-hover:text-gata-green transition-colors">{dim.alias_name}</h4>
                   {dim.transformation && <pre class="mt-4 p-3 bg-gata-dark rounded-lg text-[10px] text-gata-green/70 font-mono overflow-x-auto whitespace-pre-wrap">{dim.transformation}</pre>}
                </div>
              ))}
            </div>
          </div>
        )}

        {activeTab === 'measures' && (
          <div class="p-8 space-y-8">
            <div class="flex justify-between items-center">
              <p class="text-sm text-gata-cream/40 italic">Measures are the quantifiable numbers your business tracks.</p>
              <button type="button" onClick={() => setShowAddModal('measure')} class="bg-gata-green text-gata-dark px-4 py-2 rounded-lg font-black text-[10px] uppercase tracking-widest hover:bg-gata-hover">+ Add Formula Measure</button>
            </div>
            <div class="space-y-4">
              {Object.entries(config.measures).map(([name, measure]) => (
                <div key={name} class="p-6 bg-gata-darker border-2 border-gata-green/10 rounded-3xl group">
                  <div class="flex justify-between items-center mb-6">
                     <h4 class="text-lg font-black text-gata-cream uppercase tracking-tight">{config.dimensions[name]?.alias_name || name} <span class="text-gata-green/40">MEasures</span></h4>
                     <button type="button" onClick={() => { const c = {...config!}; delete c.measures[name]; setConfig(c); }} class="text-red-400 opacity-0 group-hover:opacity-100 transition-opacity">×</button>
                  </div>
                  <div class="flex flex-wrap gap-4">
                    {measure.aggregations.map((agg) => {
                      const type = Object.keys(agg)[0];
                      const details = agg[type] as { alias: string };
                      return (
                        <div key={details.alias} class="px-4 py-2 bg-gata-dark border border-gata-green/20 rounded-xl flex items-center gap-3">
                           <span class="text-[10px] font-black text-gata-green uppercase tracking-widest">{type}</span>
                           <span class="text-sm font-bold text-gata-cream">{details.alias}</span>
                        </div>
                      );
                    })}
                    {measure.formula && Object.entries(measure.formula).map(([key, details]: [string, SemanticFormula]) => (
                      <div key={key} class="px-4 py-2 bg-gata-dark border border-purple-500/20 rounded-xl relative group/formula">
                         <div class="flex items-center gap-3">
                            <span class="text-[10px] font-black text-purple-400 uppercase tracking-widest">Formula</span>
                            <span class="text-sm font-bold text-gata-cream">{key}</span>
                            <button type="button" onClick={() => { const c = {...config!}; delete c.measures[name].formula![key]; setConfig(c); }} class="text-red-400 opacity-0 group-hover/formula:opacity-100 transition-opacity ml-2">×</button>
                         </div>
                         <pre class="mt-2 text-[10px] text-purple-300/50 font-mono italic max-w-xs truncate">{details.sql}</pre>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {activeTab === 'preview' && (
          <div class="flex h-full w-full">
            {/* LEFT PANE: Data Preview (60%) */}
            <div class="flex flex-col border-r border-gata-green/20" style={{ width: `${splitRatio * 100}%` }}>
               <div class="flex-1 overflow-auto bg-gata-darker/20 p-4">
                   {previewRows.length > 0 ? (
                   <table class="w-full text-left text-xs text-gata-cream/80">
                       <thead class="sticky top-0 bg-gata-darker shadow-sm z-10">
                           <tr class="bg-gata-darker">{Object.keys(previewRows[0]).map(k => <th key={k} class="p-3 font-mono text-gata-green min-w-[100px]">{k}</th>)}</tr>
                       </thead>
                       <tbody class="divide-y divide-gata-green/5">
                           {previewRows.map((row, i) => (
                               <tr key={i} class="hover:bg-gata-green/5 hover:text-white transition-colors">
                                   {Object.values(row).map((v: any, j) => <td key={j} class="p-3 truncate max-w-[200px]">{String(v)}</td>)}
                               </tr>
                           ))}
                       </tbody>
                   </table>
                   ) : (
                       <div class="flex flex-col items-center justify-center h-full text-gata-cream/30 italic">
                         <span class="text-4xl mb-4">📊</span>
                         <p>No preview data available.</p>
                       </div>
                   )}
               </div>
               <div class="p-2 border-t border-gata-green/10 bg-gata-darker text-[10px] text-gata-green/40 font-mono">
                 Showing {previewRows.length} rows
               </div>
            </div>

            {/* RIGHT PANE: Chat EDA (40%) */}
            <div class="flex flex-col flex-1 bg-gata-dark">
               <div class="p-4 border-b border-gata-green/10 bg-gata-darker/50">
                  <h3 class="text-sm font-black text-gata-green uppercase tracking-widest">AI Analyst</h3>
               </div>
               
               <div class="flex-1 overflow-y-auto p-4 space-y-4">
                   {chatHistory.length === 0 && (
                       <div class="text-center py-20 px-8">
                           <div class="text-4xl mb-4">🤖</div>
                           <p class="text-gata-cream/40 text-sm">Ask me anything about the data!</p>
                       </div>
                   )}
                   {chatHistory.map((msg, i) => (
                       <div key={i} class={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                           <div class={`max-w-[90%] p-3 rounded-2xl text-xs ${msg.role === 'user' ? 'bg-gata-green/20 text-gata-green border border-gata-green/20 rounded-tr-sm' : 'bg-gata-darker text-gata-cream/90 border border-white/5 rounded-tl-sm'}`}>
                               {msg.content}
                           </div>
                       </div>
                   ))}
                   {isChatting && <div class="text-gata-green text-xs animate-pulse ml-4">Thinking...</div>}
               </div>

               <div class="p-4 border-t border-gata-green/10 bg-gata-darker/30">
                   <div class="relative">
                       <input 
                          type="text" 
                          value={userQuery}
                          onKeyDown={(e) => e.key === 'Enter' && handleChatParams()}
                          onInput={(e) => setUserQuery((e.target as HTMLInputElement).value)}
                          placeholder="Ask a question..."
                          class="w-full bg-gata-dark border border-gata-green/20 rounded-xl py-3 pl-4 pr-12 text-xs text-gata-cream focus:border-gata-green outline-none"
                       />
                       <button onClick={handleChatParams} disabled={!userQuery.trim() || isChatting} class="absolute right-2 top-2 p-1.5 text-gata-green hover:bg-gata-green/10 rounded-lg transition-all disabled:opacity-30">↑</button>
                   </div>
               </div>
            </div>
          </div>
        )}
      </div>

      {/* Add Modal */}
      {showAddModal && (
        <div class="fixed inset-0 bg-gata-dark/95 backdrop-blur-xl z-[100] flex items-center justify-center p-8 animate-in fade-in duration-300">
           <div class="max-w-2xl w-full bg-gata-darker border-4 border-gata-green rounded-[3rem] p-12 space-y-8 shadow-[0_0_100px_rgba(144,193,55,0.2)]">
              <h3 class="text-4xl font-black text-gata-cream italic tracking-tighter uppercase">Add Custom <span class="text-gata-green">{showAddModal}</span></h3>
              <div class="space-y-6">
                 <div>
                    <label class="block text-[10px] font-black text-gata-green uppercase tracking-[0.2em] mb-3">Field Alias (Human Name)</label>
                    <input type="text" value={newAlias} onChange={(e) => setNewAlias((e.target as HTMLInputElement).value)} placeholder="e.g. Sales Tier" class="w-full bg-gata-dark border-2 border-gata-green/20 rounded-2xl p-4 text-gata-cream outline-none focus:border-gata-green" />
                 </div>
                 <div>
                    <label class="block text-[10px] font-black text-gata-green uppercase tracking-[0.2em] mb-3">Target Field Name (Internal ID)</label>
                    <input type="text" value={newName} onChange={(e) => setNewName((e.target as HTMLInputElement).value)} placeholder="e.g. sales_tier_calc" class="w-full bg-gata-dark border-2 border-gata-green/20 rounded-2xl p-4 text-gata-cream outline-none focus:border-gata-green" />
                 </div>
                 <div>
                    <label class="block text-[10px] font-black text-gata-green uppercase tracking-[0.2em] mb-3">{showAddModal === 'dimension' ? 'CASE Statement / Transformation' : 'Aggregation Formula (SQL)'}</label>
                    <textarea value={newSQL} onChange={(e) => setNewSQL((e.target as HTMLTextAreaElement).value)} placeholder={showAddModal === 'dimension' ? "CASE WHEN amount > 1000 THEN 'High' ELSE 'Low' END" : "SUM(amount) / COUNT(DISTINCT user_id)"} class="w-full bg-gata-dark border-2 border-gata-green/20 rounded-2xl p-4 text-gata-cream outline-none focus:border-gata-green font-mono text-sm min-h-[150px]" />
                 </div>
              </div>
              <div class="flex gap-4 pt-4">
                 <button type="button" onClick={() => setShowAddModal(null)} class="flex-1 py-4 text-gata-cream/40 font-black uppercase tracking-widest text-sm hover:text-gata-cream">Cancel</button>
                 <button type="button" onClick={handleAddCustom} class="flex-1 py-4 bg-gata-green text-gata-dark font-black rounded-2xl shadow-xl hover:bg-gata-hover transform hover:scale-105 transition-all uppercase tracking-widest text-sm">Create Field</button>
              </div>
           </div>
        </div>
      )}
    </div>
  );
}
