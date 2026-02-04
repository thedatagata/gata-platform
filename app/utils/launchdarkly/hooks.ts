// utils/launchdarkly/hooks.ts
import { useEffect, useState } from "preact/hooks";
import { getLDClient, subscribeToFlagChanges, unsubscribeFromFlagChanges } from "./client.ts";
import { FLAGS, type FlagValues } from "./flags.ts";

/**
 * Hook to get a LaunchDarkly flag value and subscribe to updates.
 * 
 * @param flagKey The flag key from FLAGS constants
 * @param defaultValue Default value to use before initialization or if flag is missing
 * @returns The current value of the flag
 */
export function useLDFlag<K extends keyof FlagValues>(
  flagKey: K,
  defaultValue: FlagValues[K]
): FlagValues[K] {
  const [value, setValue] = useState<FlagValues[K]>(defaultValue);
  const [initialized, setInitialized] = useState(false);

  useEffect(() => {
    const client = getLDClient();
    
    // If client is ready, get initial value
    if (client) {
      const current = client.variation(flagKey, defaultValue);
      setValue(current);
      setInitialized(true);
    }

    // Subscribe to changes
    const handleChange = (newValue: FlagValues[K]) => {
      console.log(`🚩 [Hook] Flag changed: ${flagKey} =`, newValue);
      setValue(newValue);
    };

    subscribeToFlagChanges(flagKey, handleChange);

    // If not initialized, poll briefly (in case client is initializing)
    let checkInterval: number | undefined;
    if (!client) {
      checkInterval = setInterval(() => {
        const c = getLDClient();
        if (c) {
          const current = c.variation(flagKey, defaultValue);
          setValue(current);
          setInitialized(true);
          clearInterval(checkInterval);
        }
      }, 100);
    }

    return () => {
      unsubscribeFromFlagChanges(flagKey, handleChange);
      if (checkInterval) clearInterval(checkInterval);
    };
  }, [flagKey, defaultValue]);

  return value;
}

/**
 * Hook to check if LaunchDarkly is initialized
 */
export function useLDReady() {
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const check = () => {
      const client = getLDClient();
      if (client) {
        setReady(true);
        return true;
      }
      return false;
    };

    if (!check()) {
      const interval = setInterval(() => {
        if (check()) clearInterval(interval);
      }, 100);
      return () => clearInterval(interval);
    }
  }, []);

  return ready;
}
