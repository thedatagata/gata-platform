import { useState } from "preact/hooks";
import { processFileUpload } from "../../utils/system/file-service.ts";
import { getLDClient } from "../../utils/launchdarkly/client.ts";
import { FLAGS } from "../../utils/launchdarkly/flags.ts";
import { trackPerformance } from "../../utils/launchdarkly/events.ts";



interface FileUploaderProps {
  db: any; // The local DuckDB connection
  onUploadComplete: (tableName: string) => void;
}

export default function FileUploader({ db, onUploadComplete }: FileUploaderProps) {
  const [loading, setLoading] = useState(false);

  const handleFileUpload = async (event: Event) => {
    const input = event.target as HTMLInputElement;
    if (!input.files || input.files.length === 0) return;

    const file = input.files[0];
    setLoading(true);

    try {
      const ldClient = getLDClient();
      const useLocalBuffer = ldClient?.variation(FLAGS.DUCKDB_LOCAL_FILE_UPLOAD, false) ?? false;
      
      console.log(` [FileUploader] Starting upload. Local Buffer Flag: ${useLocalBuffer}`);
      const tableName = await processFileUpload(db, file, { useLocalBuffer });

      
      trackPerformance("metric", "file_upload_success", "FileUploader", { 
        fileName: file.name, 
        fileSize: file.size,
        tableName 
      });

      onUploadComplete(tableName);

    } catch (err) {
      trackPerformance("error", "file_upload_failure", "FileUploader", { 
        fileName: file.name, 
        errorMessage: (err as Error).message 
      });
      alert("Failed to load file. Check console for details.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div class="space-y-6">
      <div class="p-8 border-2 border-dashed border-gata-green/30 rounded-2xl bg-gata-darker/30 hover:border-gata-green/60 transition-all group">
        <input
          type="file"
          accept=".csv,.parquet,.json"
          onChange={handleFileUpload}
          class="hidden"
          id="file-upload"
        />
        <label 
          htmlFor="file-upload" 
          class="cursor-pointer flex flex-col items-center justify-center gap-4 py-4"
        >
          <div class="w-16 h-16 bg-gata-green/10 rounded-full flex items-center justify-center group-hover:bg-gata-green/20 transition-all">
            <span class="text-3xl"></span>
          </div>
          <div class="text-center">
            <span class="block text-xl font-bold text-gata-cream mb-1">
              {loading ? "Analyzing Data..." : "Upload Your Dataset"}
            </span>
            <span class="text-sm text-gata-cream/60">
              Drag & drop CSV, Parquet, or JSON files
            </span>
          </div>
          <div class="flex items-center gap-4 mt-2">
            <span class="px-3 py-1 bg-gata-dark text-[10px] text-gata-green/80 rounded-full border border-gata-green/20">Local Processing</span>
            <span class="px-3 py-1 bg-gata-dark text-[10px] text-gata-green/80 rounded-full border border-gata-green/20">Private & Secure</span>
          </div>
        </label>
      </div>

      <div class="bg-gata-green/5 rounded-2xl p-6 border border-gata-green/10">
        <h3 class="text-xs font-bold text-gata-green uppercase tracking-widest mb-4 flex items-center gap-2">
          <span class="text-lg"></span> Optimal Field Preparation Guide
        </h3>
        <p class="text-sm text-gata-cream/70 mb-6 leading-relaxed">
          To get the most out of the Smarter AI Analyst, ensure your dataset includes these key attributes during the profiling step:
        </p>
        
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div class="flex gap-3">
             <div class="w-8 h-8 rounded-lg bg-gata-green/20 flex items-center justify-center text-xs font-bold text-gata-green">01</div>
             <div>
                <p class="text-sm font-bold text-gata-cream">Field Name & Alias</p>
                <p class="text-[11px] text-gata-cream/50">Human-readable names (e.g., "Revenue" instead of "rev_01").</p>
             </div>
          </div>
          <div class="flex gap-3">
             <div class="w-8 h-8 rounded-lg bg-gata-green/20 flex items-center justify-center text-xs font-bold text-gata-green">02</div>
             <div>
                <p class="text-sm font-bold text-gata-cream">Data Type Category</p>
                <p class="text-[11px] text-gata-cream/50">Identify IDs, Categories, Numbers, and Time fields accurately.</p>
             </div>
          </div>
          <div class="flex gap-3">
             <div class="w-8 h-8 rounded-lg bg-gata-green/20 flex items-center justify-center text-xs font-bold text-gata-green">03</div>
             <div>
                <p class="text-sm font-bold text-gata-cream">Descriptive Context</p>
                <p class="text-[11px] text-gata-cream/50">Explain what the column represents so the AI understands logic.</p>
             </div>
          </div>
          <div class="flex gap-3">
             <div class="w-8 h-8 rounded-lg bg-gata-green/20 flex items-center justify-center text-xs font-bold text-gata-green">04</div>
             <div>
                <p class="text-sm font-bold text-gata-cream">Technical Type</p>
                <p class="text-[11px] text-gata-cream/50">Ensuring dates are detected as TIMESTAMP for trend analysis.</p>
             </div>
          </div>
        </div>
      </div>
    </div>
  );

}
