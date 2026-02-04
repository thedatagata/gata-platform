{%- macro build_ecommerce_fact(tenant_slug) -%}
{#
    Factory: Ecommerce Blended Report
    Joins Shopify orders with Stripe charges for unified revenue view.
    Links via Shopify note_attributes containing stripe_charge_id.
    
    Output schema: tenant_slug, date, order_id, order_name, charge_id,
                   total_price, amount_charged, currency, financial_status,
                   charge_status, customer_id, customer_email
#}
{%- set config = get_tenant_config(tenant_slug) -%}
{%- set sources = config.get('sources', {}) if config else {} -%}
{%- set has_shopify = sources.get('shopify', {}).get('enabled', false) -%}
{%- set has_stripe = sources.get('stripe', {}).get('enabled', false) -%}

{%- if has_shopify or has_woocommerce -%}

WITH ecommerce_orders AS (
    {% if has_shopify %}
    {{ engine_shopify_orders(tenant_slug) }}
    {% if has_woocommerce %}
    UNION ALL
    {{ engine_woocommerce_orders(tenant_slug) }}
    {% endif %}
    {% elif has_woocommerce %}
    {{ engine_woocommerce_orders(tenant_slug) }}
    {% endif %}
),

{% if has_stripe %}
stripe_charges AS (
    {{ engine_stripe_charges(tenant_slug) }}
),
{% endif %}

final AS (
    SELECT
        o.tenant_slug,
        o.date,
        o.order_id,
        o.order_name,
        o.order_created_at,
        {% if has_stripe %}
        s.charge_id,
        s.amount as amount_charged,
        s.charge_status,
        s.is_paid,
        s.card_brand,
        {% else %}
        CAST(NULL AS VARCHAR) as charge_id,
        CAST(NULL AS DOUBLE) as amount_charged,
        CAST(NULL AS VARCHAR) as charge_status,
        CAST(NULL AS BOOLEAN) as is_paid,
        CAST(NULL AS VARCHAR) as card_brand,
        {% endif %}
        o.total_price,
        o.subtotal_price,
        o.currency,
        o.financial_status,
        o.customer_id,
        o.customer_email,
        o.raw_data_payload
    FROM ecommerce_orders o
    {% if has_stripe %}
    LEFT JOIN stripe_charges s 
        ON o.stripe_charge_id = s.charge_id
    {% endif %}
)

SELECT * FROM final

{%- elif has_stripe -%}

{# Stripe-only (no Shopify) #}
SELECT
    tenant_slug,
    date,
    CAST(NULL AS BIGINT) as order_id,
    CAST(NULL AS VARCHAR) as order_name,
    CAST(NULL AS TIMESTAMP) as order_created_at,
    charge_id,
    amount as amount_charged,
    charge_status,
    is_paid,
    card_brand,
    amount as total_price,
    amount as subtotal_price,
    currency,
    charge_status as financial_status,
    CAST(NULL AS BIGINT) as customer_id,
    CAST(NULL AS VARCHAR) as customer_email,
    raw_data_payload
FROM (
    {{ engine_stripe_charges(tenant_slug) }}
)

{%- else -%}

{# No ecommerce sources #}
SELECT 
    '{{ tenant_slug }}' as tenant_slug,
    CAST(NULL AS DATE) as date,
    CAST(NULL AS BIGINT) as order_id,
    CAST(NULL AS VARCHAR) as order_name,
    CAST(NULL AS TIMESTAMP) as order_created_at,
    CAST(NULL AS VARCHAR) as charge_id,
    CAST(NULL AS DOUBLE) as amount_charged,
    CAST(NULL AS VARCHAR) as charge_status,
    CAST(NULL AS BOOLEAN) as is_paid,
    CAST(NULL AS VARCHAR) as card_brand,
    CAST(NULL AS DOUBLE) as total_price,
    CAST(NULL AS DOUBLE) as subtotal_price,
    CAST(NULL AS VARCHAR) as currency,
    CAST(NULL AS VARCHAR) as financial_status,
    CAST(NULL AS BIGINT) as customer_id,
    CAST(NULL AS VARCHAR) as customer_email,
    CAST(NULL AS JSON) as raw_data_payload
WHERE 1=0

{%- endif -%}
{%- endmacro -%}
