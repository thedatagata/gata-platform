import { JSX } from "preact";

interface IconProps {
  class?: string;
}

// Ecommerce
export function ShopifyIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2V9z" />
      <polyline points="9 22 9 12 15 12 15 22" />
    </svg>
  );
}

export function BigCommerceIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <rect x="2" y="3" width="20" height="14" rx="2" />
      <path d="M8 21h8M12 17v4" />
    </svg>
  );
}

export function WooCommerceIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <circle cx="9" cy="21" r="1" /><circle cx="20" cy="21" r="1" />
      <path d="M1 1h4l2.68 13.39a2 2 0 002 1.61h9.72a2 2 0 002-1.61L23 6H6" />
    </svg>
  );
}

// Analytics
export function GoogleAnalyticsIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M18 20V10M12 20V4M6 20v-6" stroke-linecap="round" />
    </svg>
  );
}

export function AmplitudeIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
    </svg>
  );
}

export function MixpanelIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M21.21 15.89A10 10 0 118 2.83" />
      <path d="M22 12A10 10 0 0012 2v10z" />
    </svg>
  );
}

// Ad Platforms
export function FacebookAdsIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M18 2h-3a5 5 0 00-5 5v3H7v4h3v8h4v-8h3l1-4h-4V7a1 1 0 011-1h3z" />
    </svg>
  );
}

export function GoogleAdsIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
    </svg>
  );
}

export function BingAdsIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <rect x="3" y="3" width="7" height="7" /><rect x="14" y="3" width="7" height="7" />
      <rect x="3" y="14" width="7" height="7" /><rect x="14" y="14" width="7" height="7" />
    </svg>
  );
}

export function LinkedInAdsIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M16 8a6 6 0 016 6v7h-4v-7a2 2 0 00-2-2 2 2 0 00-2 2v7h-4v-7a6 6 0 016-6z" />
      <rect x="2" y="9" width="4" height="12" /><circle cx="4" cy="4" r="2" />
    </svg>
  );
}

export function TikTokAdsIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M9 12a4 4 0 104 4V4a5 5 0 005 5" />
    </svg>
  );
}

export function AmazonAdsIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z" />
    </svg>
  );
}

export function InstagramAdsIcon({ class: className = "w-6 h-6" }: IconProps): JSX.Element {
  return (
    <svg class={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <rect x="2" y="2" width="20" height="20" rx="5" />
      <circle cx="12" cy="12" r="5" />
      <circle cx="17.5" cy="6.5" r="1.5" fill="currentColor" stroke="none" />
    </svg>
  );
}

// Lookup helper
export const CONNECTOR_ICONS: Record<string, (props: IconProps) => JSX.Element> = {
  shopify: ShopifyIcon,
  bigcommerce: BigCommerceIcon,
  woocommerce: WooCommerceIcon,
  google_analytics: GoogleAnalyticsIcon,
  amplitude: AmplitudeIcon,
  mixpanel: MixpanelIcon,
  facebook_ads: FacebookAdsIcon,
  google_ads: GoogleAdsIcon,
  bing_ads: BingAdsIcon,
  linkedin_ads: LinkedInAdsIcon,
  tiktok_ads: TikTokAdsIcon,
  amazon_ads: AmazonAdsIcon,
  instagram_ads: InstagramAdsIcon,
};
