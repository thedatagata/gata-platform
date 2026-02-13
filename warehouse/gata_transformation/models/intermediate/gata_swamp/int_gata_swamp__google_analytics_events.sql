{{ generate_intermediate_unpacker(
    tenant_slug='gata_swamp',
    source_platform='google_analytics',
    master_model_id='google_analytics_api_v1_events',
    columns=[
        {'json_key': 'event_name', 'alias': 'event_name', 'cast_to': 'VARCHAR'},
        {'json_key': 'event_date', 'alias': 'event_date', 'cast_to': 'VARCHAR'},
        {'json_key': 'event_timestamp', 'alias': 'event_timestamp', 'cast_to': 'BIGINT'},
        {'json_key': 'user_pseudo_id', 'alias': 'user_pseudo_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'user_id', 'alias': 'user_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'geo_country', 'alias': 'geo_country', 'cast_to': 'VARCHAR'},
        {'json_key': 'geo_city', 'alias': 'geo_city', 'cast_to': 'VARCHAR'},
        {'json_key': 'traffic_source_source', 'alias': 'traffic_source', 'cast_to': 'VARCHAR'},
        {'json_key': 'traffic_source_medium', 'alias': 'traffic_medium', 'cast_to': 'VARCHAR'},
        {'json_key': 'traffic_source_campaign', 'alias': 'traffic_campaign', 'cast_to': 'VARCHAR'},
        {'json_key': 'device_category', 'alias': 'device_category', 'cast_to': 'VARCHAR'},
        {'json_key': 'ga_session_id', 'alias': 'ga_session_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'ecommerce_transaction_id', 'alias': 'transaction_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'ecommerce_value', 'alias': 'purchase_revenue', 'cast_to': 'DOUBLE'},
        {'json_key': 'ecommerce_currency', 'alias': 'ecommerce_currency', 'cast_to': 'VARCHAR'}
    ]
) }}