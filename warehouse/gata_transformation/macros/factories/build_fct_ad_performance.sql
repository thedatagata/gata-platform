{#
  Factory: Ad Performance
  Unions all paid ad engines for a tenant's enabled ad sources.
  
  Usage: {{ build_fct_ad_performance('tyrell_corp', ['facebook_ads', 'google_ads', 'instagram_ads']) }}
  
  Source → Engine mapping:
    facebook_ads     → engine_facebook_ads_performance
    instagram_ads    → engine_instagram_ads_performance
    google_ads       → engine_google_ads_performance
    bing_ads         → engine_bing_ads_performance
    linkedin_ads     → engine_linkedin_ads_performance
    amazon_ads       → engine_amazon_ads_performance
    tiktok_ads       → engine_tiktok_ads_performance
#}
{% macro build_fct_ad_performance(tenant_slug, ad_sources) %}

{%- set engine_map = {
    'facebook_ads':   'engine_facebook_ads_performance',
    'instagram_ads':  'engine_instagram_ads_performance',
    'google_ads':     'engine_google_ads_performance',
    'bing_ads':       'engine_bing_ads_performance',
    'linkedin_ads':   'engine_linkedin_ads_performance',
    'amazon_ads':     'engine_amazon_ads_performance',
    'tiktok_ads':     'engine_tiktok_ads_performance'
} -%}

{%- set ns = namespace(first=true) -%}

{%- for source in ad_sources -%}
    {%- if source in engine_map -%}
        {%- if not ns.first %} UNION ALL {% endif -%}
        {{ context[engine_map[source]](tenant_slug) }}
        {%- set ns.first = false -%}
    {%- endif -%}
{%- endfor -%}

{%- if ns.first -%}
    {# No valid sources — return empty result set #}
    SELECT
        CAST(NULL AS VARCHAR) AS tenant_slug,
        CAST(NULL AS VARCHAR) AS source_platform,
        CAST(NULL AS DATE)    AS report_date,
        CAST(NULL AS VARCHAR) AS campaign_id,
        CAST(NULL AS VARCHAR) AS ad_group_id,
        CAST(NULL AS VARCHAR) AS ad_id,
        CAST(NULL AS DOUBLE)  AS spend,
        CAST(NULL AS BIGINT)  AS impressions,
        CAST(NULL AS BIGINT)  AS clicks,
        CAST(NULL AS DOUBLE)  AS conversions
    WHERE 1 = 0
{%- endif -%}

{% endmacro %}
