{{ generate_intermediate_unpacker(
    tenant_slug='wayne_enterprises',
    source_platform='bing_ads',
    master_model_id='bing_ads_api_v1_account_performance_report',
    columns=[
        {'json_key': 'time_period', 'alias': 'report_date', 'cast_to': 'DATE'},
        {'json_key': 'spend', 'alias': 'spend', 'cast_to': 'DOUBLE'},
        {'json_key': 'impressions', 'alias': 'impressions', 'cast_to': 'BIGINT'},
        {'json_key': 'clicks', 'alias': 'clicks', 'cast_to': 'BIGINT'}
    ]
) }}