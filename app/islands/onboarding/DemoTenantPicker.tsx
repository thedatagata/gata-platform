import { useState } from "preact/hooks";
import DashboardRouter from "./DashboardRouter.tsx";

interface DemoTenantPickerProps {
  motherDuckToken: string;
}

const TENANTS = [
  {
    slug: "tyrell_corp",
    name: "Tyrell Corporation",
    sources: "Facebook Ads, Instagram Ads, Google Ads, Shopify, GA4",
  },
  {
    slug: "wayne_enterprises",
    name: "Wayne Enterprises",
    sources: "Bing Ads, Google Ads, BigCommerce, GA4",
  },
  {
    slug: "stark_industries",
    name: "Stark Industries",
    sources: "Facebook Ads, Instagram Ads, WooCommerce, Mixpanel",
  },
];

export default function DemoTenantPicker({ motherDuckToken }: DemoTenantPickerProps) {
  const [selectedTenant, setSelectedTenant] = useState<string | null>(null);

  if (selectedTenant) {
    return (
      <DashboardRouter
        motherDuckToken={motherDuckToken}
        sessionId="admin-demo"
        tenantSlug={selectedTenant}
      />
    );
  }

  return (
    <div class="min-h-screen flex items-center justify-center p-4 bg-gata-dark">
      <div class="max-w-3xl w-full text-center space-y-12">
        <div>
          <h4 class="text-[10px] font-black text-gata-cream/30 uppercase tracking-[0.4em] mb-4">
            Admin Testing
          </h4>
          <h2 class="text-5xl font-black text-gata-cream italic tracking-tighter uppercase">
            Select <span class="text-gata-green">Tenant</span>
          </h2>
          <p class="text-gata-cream/40 font-medium mt-4 text-sm">
            Pick an existing tenant to test the connected analytics dashboard.
          </p>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
          {TENANTS.map((tenant) => (
            <button
              type="button"
              key={tenant.slug}
              onClick={() => setSelectedTenant(tenant.slug)}
              class="bg-gata-dark border-2 border-gata-green/10 hover:border-gata-green hover:translate-y-[-4px] p-8 rounded-2xl transition-all text-left group"
            >
              <div class="text-lg font-black text-gata-cream uppercase tracking-tight group-hover:text-gata-green transition-colors">
                {tenant.name}
              </div>
              <div class="text-[10px] text-gata-green/60 font-mono mt-2 mb-4">
                {tenant.slug}
              </div>
              <div class="text-xs text-gata-cream/40 leading-relaxed">
                {tenant.sources}
              </div>
            </button>
          ))}
        </div>

        <a
          href="/"
          class="inline-block text-gata-cream/20 hover:text-gata-green text-xs transition-colors uppercase font-bold tracking-widest"
        >
          ← Back to Home
        </a>
      </div>
    </div>
  );
}
