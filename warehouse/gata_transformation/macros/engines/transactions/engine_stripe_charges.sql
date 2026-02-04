{% macro engine_stripe_charges(tenant_slug) %}
{#
    Engine: Stripe Charges
    Input: platform_mm__stripe_api_v1_charges
    
    Mock data fields: id, amount (BIGINT cents), amount_captured, amount_refunded,
                      currency, created (TIMESTAMPTZ), status, paid (BOOL),
                      refunded (BOOL), payment_method_details__type,
                      payment_method_details__card__brand, payment_method_details__card__last4
    
    Converts amount from cents to dollars. Links to Shopify via charge ID.
#}
SELECT
    tenant_slug,
    raw_data_payload->>'id' as charge_id,
    CAST(raw_data_payload->>'created' AS TIMESTAMP) as charge_created_at,
    CAST(raw_data_payload->>'created' AS DATE) as date,
    CAST(raw_data_payload->>'amount' AS BIGINT)::DOUBLE / 100 as amount,
    CAST(raw_data_payload->>'amount_captured' AS BIGINT)::DOUBLE / 100 as amount_captured,
    CAST(raw_data_payload->>'amount_refunded' AS BIGINT)::DOUBLE / 100 as amount_refunded,
    raw_data_payload->>'currency' as currency,
    raw_data_payload->>'status' as charge_status,
    CAST(raw_data_payload->>'paid' AS BOOLEAN) as is_paid,
    CAST(raw_data_payload->>'refunded' AS BOOLEAN) as is_refunded,
    raw_data_payload->>'payment_method_details__type' as payment_type,
    raw_data_payload->>'payment_method_details__card__brand' as card_brand,
    raw_data_payload->>'payment_method_details__card__last4' as card_last4,
    raw_data_payload
FROM {{ ref('platform_mm__stripe_api_v1_charges') }}
WHERE tenant_slug = '{{ tenant_slug }}'
{% endmacro %}
