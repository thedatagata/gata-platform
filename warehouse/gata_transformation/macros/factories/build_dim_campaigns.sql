{#
  Factory: Campaign Dimension
  Builds dim_campaigns by pulling campaign metadata from each ad source's 
  intermediate campaign model.
  
  Usage: {{ build_dim_campaigns('tyrell_corp', ['facebook_ads', 'google_ads', 'instagram_ads']) }}
  
  Source → Intermediate model mapping:
    facebook_ads     → int_{slug}__facebook_ads_campaigns
    instagram_ads    → int_{slug}__instagram_ads_campaigns
    google_ads       → int_{slug}__google_ads_campaigns
    bing_ads         → int_{slug}__bing_ads_campaigns
    linkedin_ads     → int_{slug}__linkedin_ads_campaigns
    amazon_ads       → int_{slug}__amazon_ads_sponsored_products_campaigns
    tiktok_ads       → int_{slug}__tiktok_ads_campaigns
#}
{% macro build_dim_campaigns(tenant_slug, ad_sources) %}

{%- set source_table_map = {
    'facebook_ads':   'facebook_ads_campaigns',
    'instagram_ads':  'instagram_ads_campaigns',
    'google_ads':     'google_ads_campaigns',
    'bing_ads':       'bing_ads_campaigns',
    'linkedin_ads':   'linkedin_ads_campaigns',
    'amazon_ads':     'amazon_ads_sponsored_products_campaigns',
    'tiktok_ads':     'tiktok_ads_campaigns'
} -%}

{%- set ns = namespace(first=true) -%}

{%- for source in ad_sources -%}
    {%- if source in source_table_map -%}
        {%- set int_model = 'int_' ~ tenant_slug ~ '__' ~ source_table_map[source] -%}
        {%- if not ns.first %} UNION ALL {% endif -%}
        SELECT
            tenant_slug,
            source_platform,
            campaign_id,
            campaign_name,
            status AS campaign_status
        FROM {{ ref(int_model) }}
        {%- set ns.first = false -%}
    {%- endif -%}
{%- endfor -%}

{%- if ns.first -%}
    SELECT
        CAST(NULL AS VARCHAR) AS tenant_slug,
        CAST(NULL AS VARCHAR) AS source_platform,
        CAST(NULL AS VARCHAR) AS campaign_id,
        CAST(NULL AS VARCHAR) AS campaign_name,
        CAST(NULL AS VARCHAR) AS campaign_status
    WHERE 1 = 0
{%- endif -%}

{% endmacro %}
