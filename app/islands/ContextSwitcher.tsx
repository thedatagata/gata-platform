import { useState } from "preact/hooks";
import { getLDClient } from "../utils/launchdarkly/client.ts";

interface ContextSwitcherProps {
  ldClientId?: string;
}

interface LDContext {
  kind: string;
  key: string;
  name?: string;
  email?: string;
  plan_tier?: string;
  device?: string;
  custom?: Record<string, unknown>;
  [key: string]: unknown;
}

export default function ContextSwitcher({ }: ContextSwitcherProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [currentContext, setCurrentContext] = useState<LDContext>({
    kind: "user",
    key: "user-123",
    name: "Anonymous User",
    email: "user@example.com",
    plan_tier: "free",
    device: "desktop"
  });

  const updateContext = async (newContext: LDContext) => {
    const client = getLDClient();
    if (client) {
      await client.identify(newContext);
      setCurrentContext(newContext);
      console.log("Context updated:", newContext);
    } else {
      console.warn("LD Client not initialized");
    }
  };

  const presets = [
    {
      label: "Free User",
      context: {
        kind: "user",
        key: "user-free-1",
        name: "Free User",
        email: "free@example.com",
        plan_tier: "free",
        device: "desktop"
      }
    },
    {
      label: "Premium User",
      context: {
        kind: "user",
        key: "user-premium-1",
        name: "Premium User",
        email: "premium@example.com",
        plan_tier: "premium",
        device: "desktop"
      }
    },
    {
      label: "Beta Tester",
      context: {
        kind: "user",
        key: "user-beta-1",
        name: "Beta Tester",
        email: "beta@example.com",
        plan_tier: "premium",
        custom: {
          isBeta: true
        }
      }
    }
  ];

  return (
    <div class="fixed bottom-4 right-4 z-50">
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        class="bg-[#90C137] text-[#172217] px-4 py-2 rounded-full shadow-lg font-bold hover:bg-[#a0d147] transition-colors"
      >
        {isOpen ? "Close Debug" : " LD Context"}
      </button>

      {isOpen && (
        <div class="absolute bottom-12 right-0 bg-[#172217] border border-[#90C137] p-4 rounded-lg shadow-xl w-64 text-[#F8F6F0]">
          <h3 class="font-bold mb-2 text-[#90C137]">Switch Context</h3>
          <div class="space-y-2">
            {presets.map((preset) => (
              <button
                type="button"
                key={preset.label}
                onClick={() => updateContext(preset.context)}
                class={`w-full text-left px-3 py-2 rounded ${
                  currentContext.email === preset.context.email
                    ? "bg-[#90C137] text-[#172217]"
                    : "hover:bg-[#90C137]/20"
                }`}
              >
                {preset.label}
              </button>
            ))}
            
            <div class="border-t border-[#90C137]/30 my-2 pt-2">
              <button
                type="button"
                onClick={async () => {
                  // Logout via API
                  await fetch('/api/auth/logout', { method: 'POST' });
                  // Clear local storage if any
                  localStorage.clear();
                  // Reload to reset state
                  globalThis.location.href = '/app/dashboard';
                }}
                class="w-full text-left px-3 py-2 rounded hover:bg-red-900/50 text-red-300"
              >
                 Reset / New User
              </button>
            </div>
          </div>
          <div class="mt-4 pt-2 border-t border-[#90C137]/30 text-xs text-gray-400">
            Current: {currentContext.email}
          </div>
        </div>
      )}
    </div>
  );
}
