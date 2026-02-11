// utils/launchdarkly/client.ts
import * as LDClient from "launchdarkly-js-client-sdk";

let ldClient: any = null;

export async function initializeLaunchDarkly(context: any, clientSideId?: string) {
  // If provided clientSideId doesn't match current, we might need a full re-init?
  // But usually clientSideId is stable.
  
  const ldClientId = clientSideId || (globalThis as any).__LD_CLIENT_ID__;
  console.log(` [LD] Initializing with Client ID: ${ldClientId?.substring(0, 5)}...`);
  
  if (ldClient) {
    // If context changed, update it
    if (ldClient.getUser?.()?.key !== context.key) {
        console.log(` [LD] Updating context from ${ldClient.getUser?.()?.key} to ${context.key}`);
        await ldClient.identify(context);
    }
    return ldClient;
  }
  
  if (!ldClientId) {
    console.error(" [LD] LaunchDarkly client ID not configured");
    return null;
  }

  ldClient = LDClient.initialize(ldClientId, context, {
    streaming: true,
  });

  try {
    await ldClient.waitForInitialization();
    console.log(" [LD] Client successfully initialized");
  } catch (err) {
    console.error(" [LD] Client initialization failed:", err);
  }
  
  return ldClient;
}



export function getLDClient() {
  return ldClient;
}

// Real-time flag change listener
export function subscribeToFlagChanges(flagKey: string, callback: (value: any) => void) {
  if (!ldClient) return;
  
  ldClient.on(`change:${flagKey}`, callback);
  console.log(` Listening for changes to flag: ${flagKey}`);
}

export function unsubscribeFromFlagChanges(flagKey: string, callback: (value: any) => void) {
  if (!ldClient) return;
  ldClient.off(`change:${flagKey}`, callback);
}
