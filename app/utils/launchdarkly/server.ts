// utils/launchdarkly/server.ts
import * as ld from "@launchdarkly/node-server-sdk";
import { FLAGS, type FlagValues } from "./flags.ts";

let ldServerClient: ld.LDClient | null = null;

export async function initializeLDServer() {
  if (ldServerClient) return ldServerClient;
  
  const sdkKey = Deno.env.get("LAUNCHDARKLY_SDK_KEY");
  if (!sdkKey) {
    console.warn("⚠️ LaunchDarkly SDK key not configured in environment variables");
    return null;
  }

  const options: ld.LDOptions = {
    logger: {
      debug: () => {},
      info: (msg) => console.log(`[LD-Server] ${msg}`),
      warn: (msg) => console.warn(`[LD-Server] ${msg}`),
      error: (msg) => console.error(`[LD-Server] ${msg}`),
    }
  };

  ldServerClient = ld.init(sdkKey, options);
  
  try {
    await ldServerClient.waitForInitialization();
    console.log("✅ LaunchDarkly server SDK initialized");
    return ldServerClient;
  } catch (err) {
    console.error("❌ Failed to initialize LaunchDarkly server SDK:", err);
    return null;
  }
}

export function getLDServerClient() {
  return ldServerClient;
}

/**
 * Get a flag variation with type safety
 */
export async function getVariation<K extends keyof FlagValues>(
  context: ld.LDContext, 
  flagKey: K, 
  defaultValue: FlagValues[K]
): Promise<FlagValues[K]> {
  if (!ldServerClient) {
    // Try to init if missing (lazy load)
    await initializeLDServer();
    if (!ldServerClient) {
      console.warn(`[LD-Server] Client not ready, returning default for ${flagKey}`);
      return defaultValue;
    }
  }
  
  return await ldServerClient.variation(flagKey, context, defaultValue);
}

// Track custom events
export function trackEvent(context: ld.LDContext, eventKey: string, data?: any, metricValue?: number) {
  if (!ldServerClient) return;
  
  ldServerClient.track(eventKey, context, data, metricValue);
}

// Shutdown client on app exit
export async function shutdownLDServer() {
  if (ldServerClient) {
    await ldServerClient.close();
    ldServerClient = null;
  }
}
