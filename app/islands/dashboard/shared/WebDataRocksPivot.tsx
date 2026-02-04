// islands/dashboard/shared/WebDataRocksPivot.tsx
import { useEffect, useRef, useState } from "preact/hooks";
import { 
  generateSliceObject, 
  getFieldMetadata 
} from "../../../utils/smarter/dashboard_utils/slice-object-generator.ts";

import { SemanticLayer } from "../../../utils/system/semantic-profiler.ts";
import { type SemanticMetadata } from "../../../utils/smarter/dashboard_utils/semantic-config.ts";

interface WebDataRocksPivotProps {
  data: Record<string, unknown>[];
  initialSlice?: Record<string, unknown>;
  semanticLayer?: SemanticLayer | SemanticMetadata;
  onReportComplete?: (pivot: unknown) => void;
  onLoadError?: (error: string) => void;
}

export default function WebDataRocksPivot({ 
  data, 
  initialSlice, 
  semanticLayer,
  onReportComplete,
  onLoadError 
}: WebDataRocksPivotProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  // deno-lint-ignore no-explicit-any
  const pivotRef = useRef<any>(null); // WebDataRocks instance doesn't have a formal type
  const loadTimeoutRef = useRef<number | null>(null);
  const [assetsLoaded, setAssetsLoaded] = useState(false);

  // Load WebDataRocks assets dynamically
  useEffect(() => {
    if (assetsLoaded) return;

    const loadAssets = async () => {
      // Load CSS
      const cssLink = document.createElement('link');
      cssLink.rel = 'stylesheet';
      cssLink.href = '/starter/webdatarocks.min.css';
      document.head.appendChild(cssLink);

      // Load toolbar JS
      const toolbarScript = document.createElement('script');
      toolbarScript.src = '/starter/webdatarocks.toolbar.min.js';
      document.head.appendChild(toolbarScript);

      // Load main WebDataRocks JS
      const mainScript = document.createElement('script');
      mainScript.src = '/starter/webdatarocks.js';
      document.head.appendChild(mainScript);

      await new Promise((resolve) => {
        mainScript.onload = resolve;
      });

      console.log('✅ WebDataRocks assets loaded');
      setAssetsLoaded(true);
    };

    loadAssets();
  }, [assetsLoaded]);

  useEffect(() => {
    if (!assetsLoaded) {
      console.log('⏳ Waiting for WebDataRocks assets to load...');
      return;
    }
    
    if (!containerRef.current || !data || data.length === 0) {
      console.log('WebDataRocksPivot: Missing container or data', {
        hasContainer: !!containerRef.current,
        hasData: !!data,
        dataLength: data?.length || 0
      });
      return;
    }
    // deno-lint-ignore no-explicit-any
    if (typeof globalThis === 'undefined' || !(globalThis as any).WebDataRocks) {
      console.log('WebDataRocksPivot: WebDataRocks not available');
      return;
    }

    console.log('WebDataRocksPivot: Initializing', {
      rowCount: data.length,
      hasInitialSlice: !!initialSlice,
      containerReady: !!containerRef.current
    });

    // Sanitize data
    const sanitizedData = deepSanitize(data) as Record<string, unknown>[];
    
    // Test JSON serialization
    try {
      JSON.stringify(sanitizedData.slice(0, 5));
      console.log('WebDataRocksPivot: ✅ Data is serializable');
    } catch (e) {
      console.error('WebDataRocksPivot: ❌ Serialization failed:', e);
      return;
    }
    
    // deno-lint-ignore no-explicit-any
    const WebDataRocks = (globalThis as any).WebDataRocks;

    if (pivotRef.current) {
      pivotRef.current.dispose();
    }

    // Extract columns and build metadata
    const columns = sanitizedData.length > 0 ? Object.keys(sanitizedData[0]) : [];
    console.log('WebDataRocksPivot: Detected columns:', columns);

    const metadataObject = semanticLayer 
      ? buildSemanticMetadata(columns, semanticLayer)
      : buildUsersMetadata(columns);
    const dataWithMetadata = [metadataObject, ...sanitizedData];
    console.log('WebDataRocksPivot: ✅ Prepended metadata object');

    // Generate or use provided slice
    const slice = initialSlice || generateSliceObject(columns);
    console.log('WebDataRocksPivot: Generated slice:', slice);

    // Initialize with slice in the report config
    pivotRef.current = new WebDataRocks({
      container: containerRef.current,
      toolbar: true,
      height: 600,
      report: {
        dataSource: {
          data: dataWithMetadata
        },
        slice: slice,
        options: {
          showDefaultSlice: false,
          grid: {
            type: "compact",
            showTotals: "on",
            showGrandTotals: "on"
          }
        }
      }
    });

    // Set timeout to detect data overload (3 seconds)
    loadTimeoutRef.current = globalThis.setTimeout(() => {
      if (onLoadError) {
        console.error('❌ Data load timeout - likely data too large');
        onLoadError('Data load timeout - data may be too large for free tier');
      }
    }, 3000);

    // Simple reportcomplete handler
    pivotRef.current.on('reportcomplete', function() {
      console.log('📊 Report complete - slice should already be applied');
      
      // Clear timeout - report loaded successfully
      if (loadTimeoutRef.current) {
        clearTimeout(loadTimeoutRef.current);
        loadTimeoutRef.current = null;
      }

      // Verify what slice is actually active
      const currentReport = pivotRef.current.getReport();
      console.log('✅ Current slice configuration:', currentReport?.slice);

      // Debug: Show available fields to verify names match
      console.log('Available hierarchies:', pivotRef.current.getAllHierarchies());
      console.log('Available measures:', pivotRef.current.getAllMeasures());

      if (onReportComplete) {
        onReportComplete(pivotRef.current);
      }
    });

    // deno-lint-ignore no-explicit-any
    (globalThis as any).__pivotInstance = pivotRef.current;

    return () => {
      // Clear timeout on cleanup
      if (loadTimeoutRef.current) {
        clearTimeout(loadTimeoutRef.current);
        loadTimeoutRef.current = null;
      }
      
      if (pivotRef.current) {
        pivotRef.current.dispose();
        pivotRef.current = null;
      }
    };
  }, [data, initialSlice, assetsLoaded]);

  function deepSanitize(obj: unknown): unknown {
    if (obj === null || obj === undefined) return obj;
    if (typeof obj === 'bigint') return Number(obj);
    if (obj instanceof Uint8Array) {
      const view = new DataView(obj.buffer, obj.byteOffset, obj.byteLength);
      return Number(view.getBigUint64(0, true));
    }
    if (Array.isArray(obj)) return obj.map(deepSanitize);
    if (typeof obj === 'object') {
      const result: Record<string, unknown> = {};
      const actualObj = obj as Record<string, unknown>;
      for (const key in actualObj) {
        result[key] = deepSanitize(actualObj[key]);
      }
      return result;
    }
    return obj;
  }

  /**
   * Build metadata object for WebDataRocks
   * CRITICAL: First element in data array must be metadata
   * Includes availableAggregations for measures
   */
  /**
   * Build metadata object for generic SemanticLayer
   */
  function buildSemanticMetadata(columns: string[], layer: SemanticLayer | SemanticMetadata) {
    const metadataObj: Record<string, unknown> = {};
    
    columns.forEach(col => {
      // deno-lint-ignore no-explicit-any
      const field = (layer as any).fields[col];
      // deno-lint-ignore no-explicit-any
      const isMeasure = !!(layer as any).measures[col];
      
      if (isMeasure) {
        metadataObj[col] = {
          type: "number",
          availableAggregations: ["sum", "max", "min", "avg"]
        };
      } else if (field) {
        metadataObj[col] = {
          type: field.data_type_category === "numerical" || field.data_type_category === "continuous" 
            ? "number" 
            : "string"
        };
      } else {
        // Fallback
        metadataObj[col] = { type: "string" };
      }
    });

    return metadataObj;
  }

  function buildUsersMetadata(columns: string[]) {
    const metadataObj: Record<string, unknown> = {};
    
    // Field categories for aggregation rules (users table)
    const ID_FIELDS = ['user_key', 'user_id', 'email', 'first_device_id', 'last_device_id'];
    const BINARY_FLAGS = [
      'has_activated',
      'is_active_7d',
      'is_active_30d',
      'is_paying_customer'
    ];
    
    columns.forEach(col => {
      const fieldMeta = getFieldMetadata(col);
      
      if (fieldMeta) {
        // Map metadata type to WebDataRocks format
        const metaConfig: Record<string, unknown> = {
          type: fieldMeta.type,
          ...(fieldMeta.hierarchy && { hierarchy: fieldMeta.hierarchy }),
          ...(fieldMeta.parent && { parent: fieldMeta.parent })
        };
        
        // Add availableAggregations for measures
        if (fieldMeta.type === 'number') {
          if (ID_FIELDS.includes(col)) {
            metaConfig.availableAggregations = ["count", "distinctcount"];
          } else if (BINARY_FLAGS.includes(col)) {
            metaConfig.availableAggregations = ["sum", "avg"];
          } else {
            metaConfig.availableAggregations = ["sum", "max", "min", "avg"];
          }
        } else if (fieldMeta.type === 'string' && ID_FIELDS.includes(col)) {
          // String IDs also get count aggregations
          metaConfig.availableAggregations = ["count", "distinctcount"];
        }
        
        metadataObj[col] = metaConfig;
      } else {
        // Default to number type if not in metadata
        console.warn(`WebDataRocksPivot: No metadata for "${col}", defaulting to number`);
        metadataObj[col] = { 
          type: "number",
          availableAggregations: ["sum", "max", "min", "avg"]
        };
      }
    });
    
    console.log('WebDataRocksPivot: Built metadata object:', metadataObj);
    return metadataObj;
  }

  return <div ref={containerRef} style={{ width: '100%' }} />;
}
