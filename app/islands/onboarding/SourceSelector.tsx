import { useState } from "preact/hooks";
import { CONNECTOR_ICONS } from "../../components/icons/ConnectorIcons.tsx";
import { showToast } from "../app_utils/Toast.tsx";

interface SourceSelectorProps {
  tenantSlug: string;
  companyName: string;
}

type Step = 'ecommerce' | 'analytics' | 'ads' | 'review';

export default function SourceSelector({ tenantSlug, companyName }: SourceSelectorProps) {
  const [step, setStep] = useState<Step>('ecommerce');
  const [loading, setLoading] = useState(false);
  const [selections, setSelections] = useState({
    ecommerce: '',
    analytics: '',
    ads: [] as string[]
  });
  const [error, setError] = useState<string | null>(null);

  const ECOMMERCE_OPTIONS = [
    { id: 'shopify', label: 'Shopify' },
    { id: 'bigcommerce', label: 'BigCommerce' },
    { id: 'woocommerce', label: 'WooCommerce' }
  ];

  const ANALYTICS_OPTIONS = [
    { id: 'google_analytics', label: 'Google Analytics 4' },
    { id: 'amplitude', label: 'Amplitude' },
    { id: 'mixpanel', label: 'Mixpanel' }
  ];

  const ADS_OPTIONS = [
    { id: 'facebook_ads', label: 'Facebook Ads' },
    { id: 'google_ads', label: 'Google Ads' },
    { id: 'bing_ads', label: 'Bing Ads' },
    { id: 'linkedin_ads', label: 'LinkedIn Ads' },
    { id: 'tiktok_ads', label: 'TikTok Ads' },
    { id: 'amazon_ads', label: 'Amazon Ads' }
  ];

  const handleNext = () => {
    if (step === 'ecommerce' && selections.ecommerce) setStep('analytics');
    else if (step === 'analytics' && selections.analytics) setStep('ads');
    else if (step === 'ads' && selections.ads.length > 0) setStep('review');
  };

  const handleBack = () => {
    if (step === 'analytics') setStep('ecommerce');
    else if (step === 'ads') setStep('analytics');
    else if (step === 'review') setStep('ads');
  };

  const toggleAd = (id: string) => {
    if (selections.ads.includes(id)) {
      setSelections({ ...selections, ads: selections.ads.filter(a => a !== id) });
    } else {
      setSelections({ ...selections, ads: [...selections.ads, id] });
    }
  };

  const handleSubmit = async () => {
    setLoading(true);
    setError(null);
    try {
      const payload = {
        tenant_slug: tenantSlug,
        business_name: companyName,
        sources: {
          [selections.ecommerce]: { enabled: true },
          [selections.analytics]: { enabled: true },
          ...selections.ads.reduce((acc, ad) => ({ ...acc, [ad]: { enabled: true } }), {})
        }
      };

      const res = await fetch('/api/onboarding/complete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!res.ok) throw new Error('Failed to complete onboarding');

      // Redirect to dashboard (loading state handled by dashboard or phase 2)
      globalThis.location.href = '/app/dashboard?onboarding=complete';

    } catch (err) {
      showToast((err as Error).message, 'error');
      setError((err as Error).message);
      setLoading(false);
    }
  };

  return (
    <div class="max-w-2xl mx-auto p-6">
      <div class="mb-8 text-center">
        <h1 class="text-3xl font-black text-gata-cream uppercase italic tracking-tighter mb-2">
          Setup Your Data Stack
        </h1>
        <p class="text-xs text-gata-cream/40 font-mono">
          TENANT: <span class="text-gata-green">{tenantSlug}</span>
        </p>
      </div>

      <div class="bg-gata-dark/60 backdrop-blur-xl border border-gata-green/10 rounded-3xl p-8 shadow-2xl">
        
        {/* Progress System */}
        <div class="flex justify-between mb-8 border-b border-gata-green/10 pb-4">
          {['ecommerce', 'analytics', 'ads', 'review'].map((s, i) => (
             <div class={`text-[10px] uppercase font-bold tracking-[0.2em] transition-colors duration-300 ${
               ['ecommerce', 'analytics', 'ads', 'review'].indexOf(step) >= i 
               ? 'text-gata-green' 
               : 'text-gata-cream/20'
             }`}>
               0{i+1}. {s}
             </div>
          ))}
        </div>

        <div class="flex gap-3 mb-6 text-[10px] text-gata-cream/30 font-mono">
          {selections.ecommerce && <span class="text-gata-green">{selections.ecommerce}</span>}
          {selections.analytics && <span class="text-gata-green">{selections.analytics}</span>}
          {selections.ads.length > 0 && <span class="text-gata-green">{selections.ads.length} ad{selections.ads.length > 1 ? 's' : ''}</span>}
        </div>

        {error && (
            <div class="mb-6 p-4 bg-red-500/10 border border-red-500/20 rounded-2xl text-red-400 text-xs font-bold uppercase tracking-wider text-center">
              {error}
            </div>
        )}

        {/* Step 1: Ecommerce */}
        {step === 'ecommerce' && (
          <div class="space-y-4 animate-fadeIn">
            <h3 class="text-xl font-bold text-gata-cream mb-6">Select Ecommerce Platform</h3>
            <div class="grid grid-cols-1 gap-4">
              {ECOMMERCE_OPTIONS.map(opt => (
                <button
                  type="button"
                  onClick={() => setSelections({...selections, ecommerce: opt.id})}
                  class={`flex items-center gap-4 p-5 rounded-2xl border transition-all duration-300 ${
                    selections.ecommerce === opt.id 
                    ? 'bg-gata-green/20 border-gata-green text-gata-cream' 
                    : 'bg-gata-dark/40 border-gata-green/10 text-gata-cream/60 hover:border-gata-green/30'
                  }`}
                >
                  {(() => { const Icon = CONNECTOR_ICONS[opt.id]; return Icon ? <Icon class="w-7 h-7 text-gata-green" /> : null; })()}
                  <span class="font-bold tracking-wider">{opt.label}</span>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Step 2: Analytics */}
        {step === 'analytics' && (
          <div class="space-y-4 animate-fadeIn">
            <h3 class="text-xl font-bold text-gata-cream mb-6">Select Analytics Provider</h3>
            <div class="grid grid-cols-1 gap-4">
              {ANALYTICS_OPTIONS.map(opt => (
                <button
                  type="button"
                  onClick={() => setSelections({...selections, analytics: opt.id})}
                  class={`flex items-center gap-4 p-5 rounded-2xl border transition-all duration-300 ${
                    selections.analytics === opt.id 
                    ? 'bg-gata-green/20 border-gata-green text-gata-cream' 
                    : 'bg-gata-dark/40 border-gata-green/10 text-gata-cream/60 hover:border-gata-green/30'
                  }`}
                >
                  {(() => { const Icon = CONNECTOR_ICONS[opt.id]; return Icon ? <Icon class="w-7 h-7 text-gata-green" /> : null; })()}
                  <span class="font-bold tracking-wider">{opt.label}</span>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Step 3: Ads */}
        {step === 'ads' && (
          <div class="space-y-4 animate-fadeIn">
            <h3 class="text-xl font-bold text-gata-cream mb-6">Select Ad Platforms (Multi)</h3>
            <div class="grid grid-cols-2 gap-4">
              {ADS_OPTIONS.map(opt => (
                <button
                  type="button"
                  onClick={() => toggleAd(opt.id)}
                  class={`flex items-center gap-4 p-5 rounded-2xl border transition-all duration-300 ${
                    selections.ads.includes(opt.id)
                    ? 'bg-gata-green/20 border-gata-green text-gata-cream'
                    : 'bg-gata-dark/40 border-gata-green/10 text-gata-cream/60 hover:border-gata-green/30'
                  }`}
                >
                  {(() => { const Icon = CONNECTOR_ICONS[opt.id]; return Icon ? <Icon class="w-7 h-7 text-gata-green" /> : null; })()}
                  <span class="font-bold tracking-wider">{opt.label}</span>
                </button>
              ))}
            </div>
            {selections.ads.length === 0 && (
              <p class="mt-4 text-center text-red-400/80 text-xs font-bold uppercase tracking-wider">
                Select at least one ad platform to continue
              </p>
            )}
          </div>
        )}

        {/* Step 4: Review */}
        {step === 'review' && (
           <div class="space-y-6 animate-fadeIn">
             <h3 class="text-xl font-bold text-gata-cream mb-6">Review Configuration</h3>
             <div class="bg-gata-dark/80 p-6 rounded-2xl border border-gata-green/20 space-y-4">
                <div class="flex justify-between items-center">
                  <span class="text-gata-cream/40 text-xs uppercase tracking-widest">Ecommerce</span> 
                  <span class="text-gata-green font-bold">{ECOMMERCE_OPTIONS.find(o => o.id === selections.ecommerce)?.label}</span>
                </div>
                <div class="flex justify-between items-center">
                  <span class="text-gata-cream/40 text-xs uppercase tracking-widest">Analytics</span> 
                  <span class="text-gata-green font-bold">{ANALYTICS_OPTIONS.find(o => o.id === selections.analytics)?.label}</span>
                </div>
                <div>
                  <span class="text-gata-cream/40 text-xs uppercase tracking-widest block mb-2">Ad Platforms</span>
                  {selections.ads.length > 0 ? (
                    <div class="flex flex-wrap gap-2">
                      {selections.ads.map(adId => (
                         <span class="px-3 py-1 bg-gata-green/10 border border-gata-green/20 rounded-full text-[10px] text-gata-green font-bold uppercase">
                           {ADS_OPTIONS.find(o => o.id === adId)?.label}
                         </span>
                      ))}
                    </div>
                  ) : (
                    <span class="text-gata-cream/20 text-sm italic">None selected</span>
                  )}
                </div>
             </div>
           </div>
        )}

        {/* Navigation */}
        <div class="mt-8 flex justify-between pt-6 border-t border-gata-green/5">
           {step !== 'ecommerce' && (
             <button 
               type="button"
               onClick={handleBack}
               class="px-6 py-3 rounded-xl border border-gata-green/20 text-gata-cream/60 hover:text-gata-cream hover:bg-gata-green/5 transition-colors font-bold text-xs uppercase tracking-[0.2em]"
             >
               Back
             </button>
           )}
           <div class="flex-1"></div>
           {step === 'review' ? (
              <button 
                type="button"
                onClick={handleSubmit}
                disabled={loading}
                class="px-8 py-3 bg-gata-green text-gata-dark rounded-xl font-black uppercase tracking-[0.2em] text-xs hover:bg-[#a0d147] active:scale-[0.95] transition-all shadow-lg shadow-gata-green/20 disabled:opacity-50"
              >
                {loading ? 'Finalizing...' : 'Initialize Tenant'}
              </button>
           ) : (
              <button 
                type="button"
                onClick={handleNext}
                disabled={
                  (step === 'ecommerce' && !selections.ecommerce) ||
                  (step === 'analytics' && !selections.analytics) ||
                  (step === 'ads' && selections.ads.length === 0)
                }
                class="px-8 py-3 bg-gata-cream text-gata-dark rounded-xl font-black uppercase tracking-[0.2em] text-xs hover:bg-white active:scale-[0.95] transition-all disabled:opacity-30 disabled:cursor-not-allowed"
              >
                Next Step
              </button>
           )}
        </div>

      </div>
    </div>
  );
}
