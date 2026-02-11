// components/UpgradeModal.tsx
import { useEffect, useState } from "preact/hooks";
import { trackView, trackInteraction } from "../utils/launchdarkly/events.ts";
import { getLDClient } from "../utils/launchdarkly/client.ts";

interface UpgradeModalProps {
  feature: "ai_query_access" | "7b_model" | "query_persistence" | "data_limit" | "ai_analyst_access";
  trigger?: string;
  upgradeType?: "plan" | "feature"; // 'plan' = full upgrade to premium, 'feature' = unlock specific feature in current plan
  sessionId: string;
  onClose: () => void;
  onSuccess?: () => void; // Optional callback after successful upgrade
}

interface FeatureContent {
  title: string;
  description: string;
  features: string[];
  emoji: string;
}

export default function UpgradeModal({ feature, trigger, upgradeType = "plan", sessionId, onClose, onSuccess }: UpgradeModalProps) {
  const [view, setView] = useState<'upsell' | 'checkout' | 'processing' | 'success'>('upsell');

  useEffect(() => {
    // Track modal view
    trackView("modal", "upsell", "UpgradeModal", {
      plan: "starter",
      contentType: "upgrade",
      trigger: trigger || feature,
      feature,
      upgradeType
    });
  }, [feature, trigger, upgradeType]);
  
  const handleUpgradeClick = () => {
    trackInteraction("click", "upgrade_button", "upsell", "UpgradeModal", {
      plan: "starter",
      destination: "mock_checkout",
      feature,
      trigger,
      upgradeType
    });
    setView('checkout');
  };

  const handleCheckout = async () => {
    setView('processing');
    
    // Simulate processing delay
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Update LD Context via API
    try {
      const response = await fetch('/api/mock-checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sessionId,
          upgradeType,
          feature // Pass the feature parameter
        })
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.message || 'Checkout failed');
      }

      // Update local LD client immediately for instant feedback
      const client = getLDClient();
      if (client) {
        const context = client.getContext();
        if (context) {
          let newContext = { ...context };

          if (upgradeType === 'plan') {
            newContext = {
              ...newContext,
              plan_tier: 'premium',
              custom: { ...newContext.custom, plan_tier: 'premium' }
            };
          } else if (feature === 'ai_analyst_access') {
            newContext = {
              ...newContext,
              custom: { ...newContext.custom, ai_analyst_unlocked: true }
            };
          } else {
            newContext = {
              ...newContext,
              custom: { ...newContext.custom, ai_addon_unlocked: true }
            };
          }
          
          await client.identify(newContext);
          
          trackInteraction("click", "complete_purchase", "checkout", "UpgradeModal", {
            amount: upgradeType === 'plan' ? 29.00 : 9.00,
            plan: upgradeType === 'plan' ? "smarter" : "starter_plus_ai",
            upgradeType,
            feature
          });
        }
      }

      setView('success');
      
      // Auto close after success
      setTimeout(() => {
        onClose();
        // Call onSuccess callback if provided, otherwise reload
        if (onSuccess) {
          onSuccess();
        } else {
          globalThis.location.reload();
        }
      }, 2000);

    } catch (err) {
      console.error("Checkout error:", err);
      setView('upsell');
      alert(`Checkout failed: ${err instanceof Error ? err.message : 'Unknown error'}. Please try again.`);
    }
  };
  
  const handleDismiss = () => {
    trackInteraction("dismiss", "modal_close", "upsell", "UpgradeModal", {
      plan: "starter",
      feature,
      trigger
    });
    
    onClose();
  };
  
  const content: Record<string, FeatureContent> = {
    ai_query_access: {
      title: "Unlock AI-Powered Queries",
      description: "Generate SQL instantly with natural language prompts",
      features: ["Natural language to SQL", "Query suggestions", "Error corrections"],
      emoji: ""
    },
    "7b_model": {
      title: "Upgrade to 7B AI Model",
      description: "More accurate queries with our premium AI model",
      features: ["Higher accuracy", "Better context understanding", "Faster query generation"],
      emoji: ""
    },
    query_persistence: {
      title: "Save Your Queries Forever",
      description: "Keep your queries across sessions with persistent storage",
      features: ["Unlimited query storage", "Access from any device", "Query history tracking"],
      emoji: ""
    },
    data_limit: {
      title: "Need More Data?",
      description: "Upgrade to Smarter for unlimited data processing",
      features: ["Unlimited data size", "WebLLM semantic layer", "Query persistence"],
      emoji: ""
    },
    ai_analyst_access: {
      title: "Add Your Personal Data Analyst",
      description: "AI-powered insights and recommendations for your analytics",
      features: [
        "Automated pattern detection",
        "Anomaly identification", 
        "Actionable recommendations",
        "Natural language explanations"
      ],
      emoji: ""
    }
  };
  
  const currentContent = content[feature];
  const price = upgradeType === 'plan' ? "$29.00" : "$9.00";
  const planName = upgradeType === 'plan' ? "Smarter Plan (Monthly)" : "AI Add-on (Monthly)";
  const buttonText = upgradeType === 'plan' ? "Upgrade to Smarter" : "Unlock AI Features";
  
  return (
    <div 
      className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 p-4"
      onClick={handleDismiss}
    >
      <div 
        className="bg-gata-dark border-2 border-gata-green rounded-2xl shadow-2xl max-w-md w-full p-8 transform transition-all"
        onClick={(e) => e.stopPropagation()}
      >
        {view === 'upsell' && (
          <>
            <div className="text-center mb-6">
              <div className="text-6xl mb-4">{currentContent.emoji}</div>
              <h2 className="text-3xl font-bold text-gata-cream mb-2">
                {currentContent.title}
              </h2>
              <p className="text-gata-cream/80">
                {currentContent.description}
              </p>
            </div>
            
            <div className="bg-gata-dark/60 border border-gata-green/30 rounded-lg p-4 mb-6">
              <ul className="space-y-3">
                {currentContent.features.map((f, i) => (
                  <li key={i} className="flex items-start text-gata-cream">
                    <span className="text-gata-green mr-2 mt-1"></span>
                    <span>{f}</span>
                  </li>
                ))}
              </ul>
            </div>
            
            <div className="flex gap-3">
              <button
                onClick={handleUpgradeClick}
                className="flex-1 bg-gradient-to-r from-gata-green to-[#a0d147] text-gata-dark font-bold py-3 px-6 rounded-lg hover:opacity-90 transition-opacity"
              >
                {buttonText}
              </button>
              <button
                onClick={handleDismiss}
                className="px-6 py-3 border border-gata-green/50 text-gata-cream rounded-lg hover:bg-gata-green/10 transition-colors"
              >
                Maybe Later
              </button>
            </div>
          </>
        )}

        {view === 'checkout' && (
          <div className="text-center">
            <h2 className="text-2xl font-bold text-gata-cream mb-6">Mock Checkout</h2>
            <div className="bg-white/5 rounded-lg p-6 mb-6 text-left">
              <div className="flex justify-between mb-2 text-gata-cream">
                <span>{planName}</span>
                <span>{price}</span>
              </div>
              <div className="border-t border-white/10 my-2"></div>
              <div className="flex justify-between font-bold text-gata-green">
                <span>Total</span>
                <span>{price}</span>
              </div>
            </div>
            <p className="text-sm text-gray-400 mb-6">
              This is a testing environment. No actual charge will be made.
            </p>
            <button
              onClick={handleCheckout}
              className="w-full bg-gradient-to-r from-gata-green to-[#a0d147] text-gata-dark font-bold py-3 px-6 rounded-lg hover:opacity-90 transition-opacity"
            >
              Pay {price} (Mock)
            </button>
          </div>
        )}

        {view === 'processing' && (
          <div className="text-center py-8">
            <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-gata-green mx-auto mb-4"></div>
            <p className="text-gata-cream">Processing payment...</p>
          </div>
        )}

        {view === 'success' && (
          <div className="text-center py-8">
            <div className="text-6xl mb-4"></div>
            <h2 className="text-2xl font-bold text-gata-green mb-2">
              {upgradeType === 'plan' ? "Upgrade Complete!" : "Feature Unlocked!"}
            </h2>
            <p className="text-gata-cream">
              {upgradeType === 'plan' ? "Redirecting to your new dashboard..." : "Reloading to apply changes..."}
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
