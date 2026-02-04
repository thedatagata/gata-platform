{%- macro build_ads_blended_fact(tenant_slug) -%}
{#
    Factory: Ads Blended Report
    Orchestrates ad platform engines based on tenant config.
    Unions Facebook Ads + Google Ads (+ Instagram Ads if enabled) into a 
    standardized ad performance schema.
    
    Output schema: tenant_slug, source, date, campaign_name, ad_group_name,
                   campaign_id, ad_group_id, spend, impressions, clicks, 
                   conversions, utm_source, utm_medium, utm_campaign
#}
{%- set config = get_tenant_config(tenant_slug) -%}
{%- set sources = config.get('sources', {}) if config else {} -%}

{# Build list of active ad platforms #}
{%- set active_platforms = [] -%}
{%- for key in sources.keys() -%}
    {%- if key in ['facebook_ads', 'google_ads', 'instagram_ads'] and sources[key].get('enabled', false) -%}
        {%- do active_platforms.append(key) -%}
    {%- endif -%}
{%- endfor -%}

{%- if active_platforms | length > 0 -%}

{%- for platform in active_platforms -%}
    {%- set logic_config = sources[platform].get('logic', {}) -%}

    {%- if platform in ('facebook_ads', 'instagram_ads') -%}

    {# --- Facebook / Instagram Ads --- #}
    SELECT
        '{{ tenant_slug }}' as tenant_slug,
        '{{ platform }}' as source,
        i.date,
        i.campaign_name,
        COALESCE(i.adset_name, '') as ad_group_name,
        i.campaign_id,
        i.adset_id as ad_group_id,
        i.spend,
        i.impressions,
        i.clicks,
        i.conversions,
        CAST(NULL AS VARCHAR) as utm_source,
        CAST(NULL AS VARCHAR) as utm_medium,
        CAST(NULL AS VARCHAR) as utm_campaign,
        i.raw_data_payload
    FROM (
        {{ engine_facebook_ads_insights(
            tenant_slug, 
            logic_config.get('conversion_event_pattern', ''),
            platform
        ) }}
    ) i

    {%- elif platform == 'google_ads' -%}

    {# --- Google Ads --- #}
    SELECT
        '{{ tenant_slug }}' as tenant_slug,
        'google_ads' as source,
        p.date,
        p.campaign_name,
        COALESCE(p.ad_group_name, '') as ad_group_name,
        p.campaign_id,
        p.ad_group_id,
        p.spend,
        p.impressions,
        p.clicks,
        p.conversions,
        p.utm_source,
        p.utm_medium,
        p.utm_campaign,
        p.raw_data_payload
    FROM (
        {{ engine_google_ads_ad_performance(tenant_slug) }}
    ) p

    {%- endif -%}

    {%- if not loop.last %}
UNION ALL
    {%- endif -%}

{%- endfor -%}

{%- else -%}

{# No active ad platforms — return empty result set #}
SELECT 
    '{{ tenant_slug }}' as tenant_slug,
    CAST(NULL AS VARCHAR) as source,
    CAST(NULL AS DATE) as date,
    CAST(NULL AS VARCHAR) as campaign_name,
    CAST(NULL AS VARCHAR) as ad_group_name,
    CAST(NULL AS VARCHAR) as campaign_id,
    CAST(NULL AS VARCHAR) as ad_group_id,
    CAST(NULL AS DOUBLE) as spend,
    CAST(NULL AS BIGINT) as impressions,
    CAST(NULL AS BIGINT) as clicks,
    CAST(NULL AS DOUBLE) as conversions,
    CAST(NULL AS VARCHAR) as utm_source,
    CAST(NULL AS VARCHAR) as utm_medium,
    CAST(NULL AS VARCHAR) as utm_campaign,
    CAST(NULL AS JSON) as raw_data_payload
WHERE 1=0

{%- endif -%}
{%- endmacro -%}
