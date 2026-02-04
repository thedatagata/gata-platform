// utils/smarter/dashboard_utils/slice-object-generator.ts
import metadata from "../../../static/starter/users-pivot-metadata.json" with { type: "json" };

interface HierarchyField {
  uniqueName: string;
  [key: string]: unknown;
}

interface SliceObject {
  rows?: HierarchyField[];
  columns?: HierarchyField[];
  measures: Array<{
    uniqueName: string;
    aggregation: string;
    availableAggregations?: string[];
  }>;
  reportFilters?: HierarchyField[];
  expands?: { expandAll: boolean };
  drills?: { drillAll: boolean };
}

// Sequences always go to columns (for users: dates and lifecycle stages)
const SEQUENCE_FIELDS = [
  'current_lifecycle_stage'
];

// Fields that need ascending sort
const SORTED_FIELDS = {
  'current_lifecycle_stage': 'asc'
};

// Binary flags (can be rows, columns, or filters)
const BINARY_FLAGS = [
  'is_currently_anonymous',
  'has_trial_signup',
  'is_paying_customer',
  'reached_activation',
  'has_purchased_plan',
  'reached_awareness',
  'reached_consideration',
  'reached_retention',
  'is_active_30d'
];

// ID fields (usually not useful in pivots)
const ID_FIELDS = ['cookie_id'];

/**
 * Categorize query columns based on metadata
 */
export function categorizeColumns(columns: string[]): {
  sequences: string[];
  dimensions: string[];
  binaryFlags: string[];
  measures: string[];
  ids: string[];
} {
  const sequences: string[] = [];
  const dimensions: string[] = [];
  const binaryFlags: string[] = [];
  const measures: string[] = [];
  const ids: string[] = [];

  columns.forEach(col => {
    const meta = (metadata as Record<string, any>)[col];
    if (ID_FIELDS.includes(col)) {
      ids.push(col);
    } else if (SEQUENCE_FIELDS.includes(col)) {
      sequences.push(col);
    } else if (BINARY_FLAGS.includes(col)) {
      binaryFlags.push(col);
    } else if (meta?.type === 'number') {
      measures.push(col);
    } else if (meta?.type === 'level' || meta?.type === 'string') {
      dimensions.push(col);
    } else {
      // Unknown field - treat as measure
      measures.push(col);
    }
  });

  return { sequences, dimensions, binaryFlags, measures, ids };
}

/**
 * Build measure config with appropriate aggregations
 * Note: availableAggregations goes in METADATA only, not in slice
 */
function buildMeasureConfig(measureName: string) {
  const isBinary = BINARY_FLAGS.includes(measureName);
  const isId = ID_FIELDS.includes(measureName);

  if (isId) {
    // IDs use count/distinctcount
    return {
      uniqueName: measureName,
      aggregation: "distinctcount"
    };
  } else if (isBinary) {
    // Binary flags: sum (count) and avg (conversion rate)
    return {
      uniqueName: measureName,
      aggregation: "sum"
    };
  } else {
    // Regular numeric measures
    return {
      uniqueName: measureName,
      aggregation: "sum"
    };
  }
}

/**
 * Generate slice object from query columns
 * 
 * Strategy:
 * - Sequences → columns
 * - Dimensions → rows
 * - Binary flags → rows (default, can be moved to columns/filters)
 * - Measures → measures
 * - IDs → measures (count/distinctcount)
 * - Add drills/expands if hierarchy fields present
 */
export function generateSliceObject(queryColumns: string[]): SliceObject {
  const categorized = categorizeColumns(queryColumns);
  
  console.log('━━━ SLICE GENERATION ━━━');
  console.log('Sequences (→ columns):', categorized.sequences);
  console.log('Dimensions (→ rows):', categorized.dimensions);
  console.log('Binary flags (→ rows):', categorized.binaryFlags);
  console.log('Measures:', categorized.measures);
  console.log('IDs (→ measures):', categorized.ids);

  const slice: SliceObject = {
    columns: [
      ...categorized.sequences.map(name => {
        const obj: HierarchyField = { uniqueName: name };
        if (name in SORTED_FIELDS) {
          obj.sort = (SORTED_FIELDS as Record<string, string>)[name];
        }
        return obj;
      }),
      { uniqueName: "Measures" }
    ],
    rows: [
      ...categorized.dimensions.map(name => {
        const obj: HierarchyField = { uniqueName: name };
        if (name in SORTED_FIELDS) {
          obj.sort = (SORTED_FIELDS as Record<string, string>)[name];
        }
        return obj;
      }),
      ...categorized.binaryFlags.map(name => {
        const obj: HierarchyField = { uniqueName: name };
        if (name in SORTED_FIELDS) {
          obj.sort = (SORTED_FIELDS as Record<string, string>)[name];
        }
        return obj;
      })
    ],
    measures: [
      ...categorized.measures.map(buildMeasureConfig),
      ...categorized.ids.map(buildMeasureConfig)
    ]
  };

  // Check if any hierarchy fields are present
  const hasHierarchyFields = queryColumns.some(col => {
    const meta = (metadata as Record<string, any>)[col];
    return meta?.type === 'level';
  });

  // Add drills and expands for hierarchies
  if (hasHierarchyFields) {
    slice.drills = { drillAll: true };
    slice.expands = { expandAll: true };
    console.log('✅ Added drills and expands (hierarchy fields detected)');
  }

  // CRITICAL FIX: Don't delete empty arrays - WebDataRocks needs them to initialize properly
  // If we delete rows/columns, WebDataRocks won't apply the report config on initial load
  console.log('📊 Final slice structure:', {
    rows: slice.rows?.length || 0,
    columns: slice.columns?.length || 0,
    measures: slice.measures?.length || 0
  });

  return slice;
}

/**
 * Alternative: Manual slice customization
 * Allows explicit control over what goes where
 */
export function buildCustomSlice(config: {
  rows?: string[];
  columns?: string[];
  measures: string[];
  reportFilters?: string[];
  includeHierarchyOptions?: boolean;
}): SliceObject {
  const slice: SliceObject = {
    rows: config.rows?.map(name => {
      const obj: HierarchyField = { uniqueName: name };
      if (name in SORTED_FIELDS) {
        obj.sort = (SORTED_FIELDS as Record<string, string>)[name];
      }
      return obj;
    }),
    columns: [
      ...(config.columns?.map(name => {
        const obj: HierarchyField = { uniqueName: name };
        if (name in SORTED_FIELDS) {
          obj.sort = (SORTED_FIELDS as Record<string, string>)[name];
        }
        return obj;
      }) || []),
      { uniqueName: "Measures" }
    ],
    measures: config.measures.map(buildMeasureConfig),
    reportFilters: config.reportFilters?.map(name => ({ uniqueName: name }))
  };

  // Check if we should include drills/expands
  const shouldIncludeHierarchy = config.includeHierarchyOptions !== false;
  if (shouldIncludeHierarchy) {
    const allFields = [
      ...(config.rows || []),
      ...(config.columns || []),
      ...(config.reportFilters || [])
    ];
    const hasHierarchyFields = allFields.some(col => {
      const meta = (metadata as Record<string, any>)[col];
      return meta?.type === 'level';
    });

    if (hasHierarchyFields) {
      slice.drills = { drillAll: true };
      slice.expands = { expandAll: true };
    }
  }

  // Ensure arrays exist even if empty (WebDataRocks needs them for initialization)
  if (!slice.rows) slice.rows = [];
  if (!slice.columns || slice.columns.length === 0) {
    slice.columns = [{ uniqueName: "Measures" }];
  }

  return slice;
}

/**
 * Get field info from metadata
 */
export function getFieldMetadata(fieldName: string) {
  return (metadata as Record<string, any>)[fieldName];
}

/**
 * Check if field is a hierarchy level
 */
export function isHierarchyField(fieldName: string): boolean {
  const meta = (metadata as Record<string, any>)[fieldName];
  return meta?.type === 'level';
}

/**
 * Get hierarchy parent/child relationships
 */
export function getHierarchyInfo() {
  return {
    FirstTouch: {
      levels: ['first_touch_utm_campaign', 'first_touch_utm_medium', 'first_touch_utm_source']
    },
    LastTouch: {
      levels: ['last_touch_utm_campaign', 'last_touch_utm_medium', 'last_touch_utm_source']
    }
  };
}
