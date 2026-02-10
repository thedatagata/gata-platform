{%- macro get_artifact_schema() -%}
    {%- set db = target.database -%}
    {%- set sch = target.schema -%}
    {%- do return(db ~ "." ~ sch) -%}
{%- endmacro -%}


{%- macro _safe_str(val) -%}
    {{- val | string | replace("\\", "\\\\") | replace("'", "''") -}}
{%- endmacro -%}


{%- macro create_model_artifacts_table() -%}
    {%- set schema_ref = get_artifact_schema() -%}
    {%- do run_query("CREATE SCHEMA IF NOT EXISTS " ~ schema_ref) -%}
    {%- set query -%}
        CREATE TABLE IF NOT EXISTS {{ schema_ref }}.model_artifacts__current (
            invocation_id VARCHAR,
            node_id VARCHAR,
            model_name VARCHAR,
            description VARCHAR,
            materialization VARCHAR,
            schema_name VARCHAR,
            tags VARCHAR,
            meta VARCHAR,
            config VARCHAR,
            depends_on_nodes VARCHAR,
            extracted_at TIMESTAMP
        )
    {%- endset -%}
    {%- do run_query(query) -%}
    {%- do log("[OBS] Created table: " ~ schema_ref ~ ".model_artifacts__current", info=True) -%}
{%- endmacro -%}


{%- macro create_run_results_table() -%}
    {%- set schema_ref = get_artifact_schema() -%}
    {%- set query -%}
        CREATE TABLE IF NOT EXISTS {{ schema_ref }}.run_results__current (
            invocation_id VARCHAR,
            node_id VARCHAR,
            node_name VARCHAR,
            resource_type VARCHAR,
            status VARCHAR,
            message VARCHAR,
            rows_affected BIGINT,
            execution_time_seconds DOUBLE,
            run_started_at TIMESTAMP,
            run_completed_at TIMESTAMP,
            extracted_at TIMESTAMP
        )
    {%- endset -%}
    {%- do run_query(query) -%}
    {%- do log("[OBS] Created table: " ~ schema_ref ~ ".run_results__current", info=True) -%}
{%- endmacro -%}


{%- macro create_test_artifacts_table() -%}
    {%- set schema_ref = get_artifact_schema() -%}
    {%- set query -%}
        CREATE TABLE IF NOT EXISTS {{ schema_ref }}.test_artifacts__current (
            invocation_id VARCHAR,
            node_id VARCHAR,
            test_name VARCHAR,
            test_type VARCHAR,
            description VARCHAR,
            tags VARCHAR,
            test_metadata VARCHAR,
            target_model_ids VARCHAR,
            extracted_at TIMESTAMP
        )
    {%- endset -%}
    {%- do run_query(query) -%}
    {%- do log("[OBS] Created table: " ~ schema_ref ~ ".test_artifacts__current", info=True) -%}
{%- endmacro -%}


{%- macro init_artifact_tables() -%}
    {%- set schema_ref = get_artifact_schema() -%}
    {%- set relation = adapter.get_relation(
            database=target.database,
            schema=target.schema,
            identifier='run_results__current'
    ) -%}
    {%- if flags.full_refresh or relation is none -%}
        {%- do log("[OBS] Initializing artifact tables in: " ~ schema_ref, info=True) -%}
        {%- do run_query("DROP TABLE IF EXISTS " ~ schema_ref ~ ".model_artifacts__current") -%}
        {%- do run_query("DROP TABLE IF EXISTS " ~ schema_ref ~ ".run_results__current") -%}
        {%- do run_query("DROP TABLE IF EXISTS " ~ schema_ref ~ ".test_artifacts__current") -%}
        {%- do create_model_artifacts_table() -%}
        {%- do create_run_results_table() -%}
        {%- do create_test_artifacts_table() -%}
    {%- endif -%}
{%- endmacro -%}


{%- macro upload_model_definitions() -%}
    {%- set target_table = get_artifact_schema() ~ ".model_artifacts__current" -%}
    {%- set relation = adapter.get_relation(
            database=target.database,
            schema=target.schema,
            identifier='model_artifacts__current'
    ) -%}
    {%- if relation is none -%}
        {%- do log("[OBS] model_artifacts__current not found, skipping upload", info=True) -%}
        {%- do return('') -%}
    {%- endif -%}

    {%- do run_query("TRUNCATE TABLE " ~ target_table) -%}

    {%- set models = [] -%}
    {%- if execute -%}
        {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") -%}
            {%- set model_data = {
                'invocation_id': invocation_id,
                'node_id': node.unique_id,
                'model_name': node.name,
                'description': _safe_str(node.description | default('')),
                'materialization': node.config.get('materialized', 'n/a'),
                'schema_name': node.schema,
                'tags': _safe_str(tojson(node.tags | default([]))),
                'meta': _safe_str(tojson(node.config.meta | default({}))),
                'config': _safe_str(tojson(node.config | default({}))),
                'depends_on_nodes': _safe_str(tojson(node.depends_on.nodes | default([])))
            } -%}
            {%- do models.append(model_data) -%}
        {%- endfor -%}
    {%- endif -%}

    {# Batch inserts in groups of 25 to avoid DuckDB parameter limits #}
    {%- set batch_size = 25 -%}
    {%- for batch_start in range(0, models | length, batch_size) -%}
        {%- set batch = models[batch_start:batch_start + batch_size] -%}
        {%- set insert_query -%}
            INSERT INTO {{ target_table }}
                (invocation_id, node_id, model_name, description, materialization,
                 schema_name, tags, meta, config, depends_on_nodes, extracted_at)
            VALUES
            {%- for m in batch -%}
                ('{{ m.invocation_id }}',
                 '{{ m.node_id }}',
                 '{{ m.model_name }}',
                 '{{ m.description }}',
                 '{{ m.materialization }}',
                 '{{ m.schema_name }}',
                 '{{ m.tags }}',
                 '{{ m.meta }}',
                 '{{ m.config }}',
                 '{{ m.depends_on_nodes }}',
                 NOW()){{ "," if not loop.last }}
            {%- endfor -%}
        {%- endset -%}
        {%- do run_query(insert_query) -%}
    {%- endfor -%}

    {%- if models | length > 0 -%}
        {%- do log("[OBS] Uploaded " ~ models | length ~ " model definitions", info=True) -%}
    {%- endif -%}
{%- endmacro -%}


{%- macro upload_run_results() -%}
    {%- set target_table = get_artifact_schema() ~ ".run_results__current" -%}
    {%- set relation = adapter.get_relation(
            database=target.database,
            schema=target.schema,
            identifier='run_results__current'
    ) -%}
    {%- if relation is none -%}
        {%- do log("[OBS] run_results__current not found, skipping upload", info=True) -%}
        {%- do return('') -%}
    {%- endif -%}

    {%- do run_query("TRUNCATE TABLE " ~ target_table) -%}

    {%- set runs = [] -%}
    {%- if results is defined -%}
        {%- for res in results -%}
            {%- set run_data = {
                'invocation_id': invocation_id,
                'node_id': res.node.unique_id,
                'node_name': res.node.name,
                'resource_type': res.node.resource_type,
                'status': res.status,
                'message': _safe_str(res.message | default('')),
                'rows_affected': res.adapter_response.get('rows_affected', 0) | default(0),
                'execution_time_seconds': res.execution_time | default(0),
                'run_started_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S'),
                'run_completed_at': modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            } -%}
            {%- do runs.append(run_data) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set batch_size = 25 -%}
    {%- for batch_start in range(0, runs | length, batch_size) -%}
        {%- set batch = runs[batch_start:batch_start + batch_size] -%}
        {%- set insert_query -%}
            INSERT INTO {{ target_table }}
                (invocation_id, node_id, node_name, resource_type, status, message,
                 rows_affected, execution_time_seconds, run_started_at, run_completed_at, extracted_at)
            VALUES
            {%- for r in batch -%}
                ('{{ r.invocation_id }}',
                 '{{ r.node_id }}',
                 '{{ r.node_name }}',
                 '{{ r.resource_type }}',
                 '{{ r.status }}',
                 '{{ r.message }}',
                 {{ r.rows_affected }},
                 {{ r.execution_time_seconds }},
                 '{{ r.run_started_at }}'::TIMESTAMP,
                 '{{ r.run_completed_at }}'::TIMESTAMP,
                 NOW()){{ "," if not loop.last }}
            {%- endfor -%}
        {%- endset -%}
        {%- do run_query(insert_query) -%}
    {%- endfor -%}

    {%- if runs | length > 0 -%}
        {%- do log("[OBS] Uploaded " ~ runs | length ~ " run results", info=True) -%}
    {%- endif -%}
{%- endmacro -%}


{%- macro upload_test_definitions() -%}
    {%- set target_table = get_artifact_schema() ~ ".test_artifacts__current" -%}
    {%- set relation = adapter.get_relation(
            database=target.database,
            schema=target.schema,
            identifier='test_artifacts__current'
    ) -%}
    {%- if relation is none -%}
        {%- do log("[OBS] test_artifacts__current not found, skipping upload", info=True) -%}
        {%- do return('') -%}
    {%- endif -%}

    {%- do run_query("TRUNCATE TABLE " ~ target_table) -%}

    {%- set tests = [] -%}
    {%- if execute -%}
        {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "test") -%}
            {%- set is_generic = node.test_metadata is defined and node.test_metadata -%}
            {%- set test_data = {
                'invocation_id': invocation_id,
                'node_id': node.unique_id,
                'test_name': node.name,
                'test_type': 'generic' if is_generic else 'singular',
                'description': _safe_str(node.description | default('')),
                'tags': _safe_str(tojson(node.tags | default([]))),
                'test_metadata': _safe_str(tojson(node.test_metadata | default({}))),
                'target_model_ids': _safe_str(tojson(node.depends_on.nodes | default([])))
            } -%}
            {%- do tests.append(test_data) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set batch_size = 25 -%}
    {%- for batch_start in range(0, tests | length, batch_size) -%}
        {%- set batch = tests[batch_start:batch_start + batch_size] -%}
        {%- set insert_query -%}
            INSERT INTO {{ target_table }}
                (invocation_id, node_id, test_name, test_type, description,
                 tags, test_metadata, target_model_ids, extracted_at)
            VALUES
            {%- for t in batch -%}
                ('{{ t.invocation_id }}',
                 '{{ t.node_id }}',
                 '{{ t.test_name }}',
                 '{{ t.test_type }}',
                 '{{ t.description }}',
                 '{{ t.tags }}',
                 '{{ t.test_metadata }}',
                 '{{ t.target_model_ids }}',
                 NOW()){{ "," if not loop.last }}
            {%- endfor -%}
        {%- endset -%}
        {%- do run_query(insert_query) -%}
    {%- endfor -%}

    {%- if tests | length > 0 -%}
        {%- do log("[OBS] Uploaded " ~ tests | length ~ " test definitions", info=True) -%}
    {%- endif -%}
{%- endmacro -%}
