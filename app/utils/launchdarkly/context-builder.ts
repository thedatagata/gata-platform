// utils/launchdarkly/context-builder.ts
export interface UserContext {
  kind: "user";
  key: string;  // email or user ID
  email: string;
  name?: string;
  custom: {
    // Subscription info
    plan: "free" | "starter" | "premium";
    planStartDate?: string;
    
    // Usage metrics
    queriesExecuted: number;
    dashboardsCreated: number;
    lastActiveDate: string;
    
    // User attributes
    role: "viewer" | "analyst" | "admin";
    companySize?: "solo" | "small" | "medium" | "enterprise";
    industry?: string;
    preferredModelTier?: "3b" | "7b";
    
    // Technical context
    preferredModel?: string;
    avgQueryComplexity?: "simple" | "medium" | "complex";
  };
}

export interface SessionContext {
  kind: "session";
  key: string;  // session ID
  custom: {
    // Session info
    startTime: string;
    deviceType: "desktop" | "mobile" | "tablet";
    
    // Current activity
    currentDashboard: "starter" | "smarter" | "none";
    queriesThisSession: number;
    errorsThisSession: number;
    
    // Feature usage
    usedCache: boolean;
    usedAI: boolean;
    usedSemanticLayer: boolean;
  };
}

export function buildMultiContext(
  user: Partial<UserContext>,
  session: Partial<SessionContext>
): any {
  return {
    kind: "multi",
    user: {
      kind: "user",
      key: user.key || "anonymous",
      ...user
    },
    session: {
      kind: "session",
      key: session.key || crypto.randomUUID(),
      ...session
    }
  };
}

/**
 * Simple user context builder for initial LaunchDarkly routing
 */
export function buildUserContext(email: string, preferredModelTier: "3b" | "7b" = "3b"): UserContext {
  return {
    kind: "user",
    key: email,
    email: email,
    custom: {
      plan: "free",
      queriesExecuted: 0,
      dashboardsCreated: 0,
      lastActiveDate: new Date().toISOString(),
      role: "viewer",
      preferredModelTier: preferredModelTier
    }
  };
}
