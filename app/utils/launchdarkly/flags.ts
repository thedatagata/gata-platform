// utils/launchdarkly/flags.ts

/**
 * Central source of truth for all LaunchDarkly feature flags.
 * Use these constants instead of hardcoded strings to prevent typos.
 */
export const FLAGS = {
  // Design & UI
  NEW_HERO_DESIGN: "new-hero-design",
  
  // AI & Models
  SMARTER_MODEL_TIER: "smarter-model-tier",
  AI_ANALYST_ACCESS: "smarter-ai-analyst-access",
  QUERY_VALIDATION_STRATEGY: "query-validation-strategy",
  
  // Features
  QUERY_PERSISTENCE: "query-persistence",
  DATA_LIMIT: "data-limit",
  
  // Experiments
  AI_MODEL_PERFORMANCE: "ai-model-performance", // Metric key
  
  // Access Control
  DEMO_ACCESS_ALLOWLIST: "demo-access-allowlist",
  DEMO_QUERY_LIMIT: "demo-query-limit",
  
  // Guarded Rollouts
  DUCKDB_LOCAL_FILE_UPLOAD: "duckdb-local-file-upload",
  CUSTOM_DATA_ONBOARDING_ENABLED: "custom-data-onboarding-enabled",
} as const;



// Type for flag values to ensure type safety
export interface FlagValues {
  [FLAGS.NEW_HERO_DESIGN]: boolean;
  [FLAGS.SMARTER_MODEL_TIER]: "3b" | "7b";
  [FLAGS.AI_ANALYST_ACCESS]: boolean;
  [FLAGS.QUERY_VALIDATION_STRATEGY]: "strict" | "adaptive" | "fast";
  [FLAGS.QUERY_PERSISTENCE]: boolean;
  [FLAGS.DATA_LIMIT]: number;
  [FLAGS.DEMO_ACCESS_ALLOWLIST]: boolean;
  [FLAGS.DEMO_QUERY_LIMIT]: number;
  [FLAGS.DUCKDB_LOCAL_FILE_UPLOAD]: boolean;
  [FLAGS.CUSTOM_DATA_ONBOARDING_ENABLED]: boolean;
}


