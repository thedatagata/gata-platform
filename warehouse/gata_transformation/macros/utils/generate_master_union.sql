{% macro generate_master_union(master_type) %}
    {# 
       Mapping concept names to physical master tables.
    #}
    {%- set mappings = {
        'campaigns': [
            'platform_mm__facebook_ads_api_v1_campaigns',
            'platform_mm__google_ads_api_v1_campaigns',
            'platform_mm__bing_ads_api_v1_campaigns',
            'platform_mm__linkedin_ads_api_v1_campaigns',
            'platform_mm__amazon_ads_api_v1_sponsored_products_campaigns'
        ],
        'products': [
             'platform_mm__shopify_api_v1_products',
             'platform_mm__bigcommerce_api_v1_products',
             'platform_mm__woocommerce_api_v1_products'
        ],
        'orders': [
             'platform_mm__shopify_api_v1_orders',
             'platform_mm__bigcommerce_api_v1_orders',
             'platform_mm__woocommerce_api_v1_orders'
        ],
        'ad_performance': [
             'platform_mm__facebook_ads_api_v1_facebook_insights',
             'platform_mm__google_ads_api_v1_ad_performance',
             'platform_mm__bing_ads_api_v1_account_performance_report', 
             'platform_mm__linkedin_ads_api_v1_ad_analytics_by_campaign'
        ]
    } -%}

    {%- set tables = mappings.get(master_type, []) -%}

    {%- if not tables -%}
        SELECT 'No tables found for type: {{ master_type }}' as error
    {%- else -%}
        {%- for table in tables -%}
             SELECT *, '{{ table }}' as _table_source FROM {{ ref(table) }}
             {%- if not loop.last %} UNION ALL {% endif -%}
        {%- endfor -%}
    {%- endif -%}
{% endmacro %}
