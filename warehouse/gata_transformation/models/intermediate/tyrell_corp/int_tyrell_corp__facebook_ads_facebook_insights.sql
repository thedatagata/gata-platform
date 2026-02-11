{{ generate_intermediate_unpacker(
    tenant_slug='tyrell_corp',
    source_platform='facebook_ads',
    master_model_id='facebook_ads_api_v1_facebook_insights',
    columns=[
        {'json_key': 'date_start', 'alias': 'report_date', 'cast_to': 'DATE'},
        {'json_key': 'spend', 'alias': 'spend', 'cast_to': 'DOUBLE'},
        {'json_key': 'impressions', 'alias': 'impressions', 'cast_to': 'BIGINT'},
        {'json_key': 'clicks', 'alias': 'clicks', 'cast_to': 'BIGINT'},
        {'json_key': 'conversions', 'alias': 'conversions', 'cast_to': 'DOUBLE'},
        {'json_key': 'campaign_id', 'alias': 'campaign_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'adset_id', 'alias': 'adset_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'ad_id', 'alias': 'ad_id', 'cast_to': 'VARCHAR'}
    ]
) }}