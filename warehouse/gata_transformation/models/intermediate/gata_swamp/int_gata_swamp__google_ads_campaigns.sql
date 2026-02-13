{{ generate_intermediate_unpacker(
    tenant_slug='gata_swamp',
    source_platform='google_ads',
    master_model_id='google_ads_api_v1_campaigns',
    columns=[
        {'json_key': 'id', 'alias': 'campaign_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'name', 'alias': 'campaign_name', 'cast_to': 'VARCHAR'},
        {'json_key': 'status', 'alias': 'status', 'cast_to': 'VARCHAR'}
    ]
) }}