{{ generate_intermediate_unpacker(
    'wayne_enterprises', 'bigcommerce', 'bigcommerce_api_v1_orders',
    [
        {'json_key': 'id', 'alias': 'order_id', 'cast_to': 'BIGINT'},
        {'json_key': 'created_at', 'alias': 'order_created_at', 'cast_to': 'TIMESTAMP'},
        {'json_key': 'total_price', 'alias': 'total_price', 'cast_to': 'DOUBLE'},
        {'json_key': 'currency', 'alias': 'currency', 'cast_to': 'VARCHAR'},
        {'json_key': 'status', 'alias': 'order_status', 'cast_to': 'VARCHAR'},
        {'json_key': 'customer_id', 'alias': 'customer_id', 'cast_to': 'VARCHAR'}
    ]
) }}
