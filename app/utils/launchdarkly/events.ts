// utils/launchdarkly/events.ts
import { getLDClient } from "./client.ts";

// ============ CLIENT EVENTS ============

/**
 * EVENT: view
 * Tracks anything displayed to the user
 */
export function trackView(
  type: "page" | "route" | "island" | "modal" | "section" | "results" | "preview" | "gate",
  context: string,
  component: string,
  additionalProps: Record<string, any> = {}
) {
  const client = getLDClient();
  if (!client) return;

  client.track("view", {
    type,
    context,
    component,
    plan: additionalProps.plan || getCurrentPlan(),
    route: typeof window !== "undefined" ? window.location.pathname : null,
    previousRoute: getPreviousRoute(),
    referrer: typeof document !== "undefined" ? document.referrer : null,
    contentType: null,
    itemCount: null,
    loadTimeMs: null,
    ...additionalProps
  });
  
  console.log(`👁️ [VIEW] ${type}:${context}:${component}`, additionalProps);
}

/**
 * EVENT: interaction
 * Tracks user-initiated actions
 */
export function trackInteraction(
  action: "click" | "submit" | "navigate" | "switch" | "drag" | "hover" | "dismiss",
  target: string,
  context: string,
  component: string,
  additionalProps: Record<string, any> = {}
) {
  const client = getLDClient();
  if (!client) return;

  client.track("interaction", {
    action,
    target,
    context,
    component,
    plan: additionalProps.plan || getCurrentPlan(),
    route: typeof window !== "undefined" ? window.location.pathname : null,
    destination: null,
    value: null,
    previousValue: null,
    timeInContext: getTimeInContext(),
    ...additionalProps
  });
  
  console.log(`🖱️ [INTERACTION] ${action}:${target}`, additionalProps);
}

/**
 * EVENT: performance
 * Tracks metrics, warnings, errors
 */
export function trackPerformance(
  type: "load" | "error" | "warning" | "metric",
  context: string,
  component: string,
  additionalProps: Record<string, any> = {}
) {
  const client = getLDClient();
  if (!client) return;

  client.track("performance", {
    type,
    context,
    component,
    plan: additionalProps.plan || getCurrentPlan(),
    metric: null,
    value: null,
    threshold: null,
    success: null,
    severity: null,
    errorType: null,
    errorMessage: null,
    ...additionalProps
  });
  
  console.log(`⚡ [PERFORMANCE] ${type}:${context}`, additionalProps);
}

/**
 * EVENT: session
 * Tracks user session lifecycle
 */
export function trackSession(
  type: "start" | "end" | "idle",
  additionalProps: Record<string, any> = {}
) {
  const client = getLDClient();
  if (!client) return;

  client.track("session", {
    type,
    plan: additionalProps.plan || getCurrentPlan(),
    sessionId: getSessionId(),
    isReturning: null,
    daysSinceLastVisit: null,
    entryPage: null,
    exitPage: null,
    referrer: null,
    sessionDuration: null,
    queriesExecuted: null,
    chartsGenerated: null,
    pagesViewed: null,
    interactionsCount: null,
    idleDuration: null,
    lastAction: null,
    ...additionalProps
  });
  
  console.log(`🕒 [SESSION] ${type}`, additionalProps);
}

// ============ HELPER FUNCTIONS ============

function getCurrentPlan(): "starter" | "smarter" | null {
  if (typeof sessionStorage === "undefined") return null;
  return sessionStorage.getItem("currentPlan") as any || null;
}

function getPreviousRoute(): string | null {
  if (typeof sessionStorage === "undefined") return null;
  return sessionStorage.getItem("previousRoute") || null;
}

function getTimeInContext(): number {
  if (typeof sessionStorage === "undefined") return 0;
  const start = sessionStorage.getItem("contextStartTime");
  return start ? Date.now() - parseInt(start) : 0;
}

export function getSessionId(): string {

  if (typeof sessionStorage === "undefined") return "server-session";
  let id = sessionStorage.getItem("sessionId");
  if (!id) {
    id = crypto.randomUUID();
    sessionStorage.setItem("sessionId", id);
  }
  return id;
}

// Store route changes
export function updateRouteTracking(newRoute: string) {
  if (typeof window === "undefined" || typeof sessionStorage === "undefined") return;
  const current = window.location.pathname;
  sessionStorage.setItem("previousRoute", current);
  sessionStorage.setItem("contextStartTime", Date.now().toString());
}

// ============ SERVER EVENTS (for API routes) ============

/**
 * EVENT: operation
 * Tracks backend processes
 */
export async function trackServerOperation(
  type: "query" | "generation" | "materialization" | "computation",
  service: "duckdb" | "motherduck" | "semantic_layer" | "webllm",
  action: string,
  additionalProps: Record<string, any> = {}
) {
  // Server-side tracking implementation
  // Will call LaunchDarkly server SDK
  console.log(`⚙️ [OPERATION] ${service}:${type}:${action}`, additionalProps);
}

/**
 * EVENT: error
 * Server failures requiring immediate attention
 */
export async function trackServerError(
  type: "timeout" | "exception" | "validation" | "connection",
  service: string,
  additionalProps: Record<string, any> = {}
) {
  console.error(`❌ [ERROR] ${service}:${type}`, additionalProps);
}
