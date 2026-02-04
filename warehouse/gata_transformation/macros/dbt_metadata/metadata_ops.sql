{%- macro get_artifact_schema() -%}
{%- set db = target.database -%}{%- set sch = target.schema -%}
{%- if target.type == 'bigquery' -%}{%- do return("`" ~ db ~ "`." ~ sch) -%}{%- else -%}{%- do return(db ~ "." ~ sch) -%}{%- endif -%}
{%- endmacro -%}

{%- macro create_model_artifacts_table() -%}
{%- set schema_ref = get_artifact_schema() -%}{%- do run_query("create schema if not exists " ~ schema_ref) -%}
{%- set query -%}
create table {{ schema_ref }}.model_artifacts__current (invocation_id VARCHAR, node_id VARCHAR, model_name VARCHAR, description VARCHAR, materialization VARCHAR, schema_name VARCHAR, tags VARCHAR, meta VARCHAR, config VARCHAR, depends_on_nodes VARCHAR, extracted_at timestamp);
{%- endset -%}{%- do run_query(query) -%}{%- do log("Created table: " ~ schema_ref ~ ".model_artifacts__current", info=True) -%}
{%- endmacro -%}

{%- macro create_run_results_table() -%}
{%- set schema_ref = get_artifact_schema() -%}
{%- set query -%}
create table {{ schema_ref }}.run_results__current (invocation_id VARCHAR, node_id VARCHAR, node_name VARCHAR, resource_type VARCHAR, status VARCHAR, message VARCHAR, rows_affected BIGINT, execution_time_seconds DOUBLE, run_started_at timestamp, run_completed_at timestamp, extracted_at timestamp);
{%- endset -%}{%- do run_query(query) -%}{%- do log("Created table: " ~ schema_ref ~ ".run_results__current", info=True) -%}
{%- endmacro -%}

{%- macro create_test_artifacts_table() -%}
{%- set schema_ref = get_artifact_schema() -%}
{%- set query -%}
create table {{ schema_ref }}.test_artifacts__current (invocation_id VARCHAR, node_id VARCHAR, test_name VARCHAR, test_type VARCHAR, description VARCHAR, tags VARCHAR, test_metadata VARCHAR, target_model_ids VARCHAR, extracted_at timestamp);
{%- endset -%}{%- do run_query(query) -%}{%- do log("Created table: " ~ schema_ref ~ ".test_artifacts__current", info=True) -%}
{%- endmacro -%}

{%- macro init_artifact_tables() -%}
{%- set schema_ref = get_artifact_schema() -%}{%- set relation = adapter.get_relation(database=target.database, schema=target.schema, identifier='model_artifacts__current') -%}
{%- if flags.full_refresh or relation is none -%}
{%- do log("Initializing environment: " ~ schema_ref, info=True) -%}
{%- do run_query("drop table if exists " ~ schema_ref ~ ".model_artifacts__current") -%}
{%- do run_query("drop table if exists " ~ schema_ref ~ ".run_results__current") -%}
{%- do run_query("drop table if exists " ~ schema_ref ~ ".test_artifacts__current") -%}
{%- do create_model_artifacts_table() -%}{%- do create_run_results_table() -%}{%- do create_test_artifacts_table() -%}
{%- endif -%}
{%- endmacro -%}

{%- macro upload_model_definitions() -%}
{%- set target_table = get_artifact_schema() ~ ".model_artifacts__current" -%}
{%- set relation = adapter.get_relation(database=target.database, schema=target.schema, identifier='model_artifacts__current') -%}
{%- if relation is not none -%}
{%- do run_query("truncate table " ~ target_table) -%}{%- set models = [] -%}
{%- if execute -%}
{%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") -%}
{%- set model_data = {'invocation_id': invocation_id, 'node_id': node.unique_id, 'model_name': node.name, 'description': node.description | replace('"', "'"), 'materialization': node.config.get('materialized', 'n/a'), 'schema_name': node.schema, 'tags': tojson(node.tags | default([])), 'meta': tojson(node.config.meta | default({})), 'config': tojson(node.config | default({})) , 'depends_on_nodes': tojson(node.depends_on.nodes | default([]))} -%}{%- do models.append(model_data) -%}
{%- endfor -%}
{%- endif -%}
{%- if models|length > 0 -%}
{%- set insert_query -%}
insert into {{ target_table }} (invocation_id, node_id, model_name, description, materialization, schema_name, tags, meta, config, depends_on_nodes, extracted_at) values
{%- for m in models -%}
('{{ m.invocation_id }}', '{{ m.node_id }}', '{{ m.model_name }}', '{{ m.description | replace("'", "''") }}', '{{ m.materialization }}', '{{ m.schema_name }}', '{{ m.tags }}', '{{ m.meta | replace("'", "''") }}', '{{ m.config | replace("'", "''") }}', '{{ m.depends_on_nodes }}', now()){{ "," if not loop.last }}
{%- endfor -%}
{%- endset -%}{%- do run_query(insert_query) -%}{%- do log("Uploaded model definitions to " ~ target_table, info=True) -%}
{%- endif -%}{%- endif -%}
{%- endmacro -%}

{%- macro upload_run_results() -%}
{%- set target_table = get_artifact_schema() ~ ".run_results__current" -%}
{%- set relation = adapter.get_relation(database=target.database, schema=target.schema, identifier='run_results__current') -%}
{%- if relation is not none -%}
{%- do run_query("truncate table " ~ target_table) -%}{%- set runs = [] -%}
{%- if results is defined -%}
{%- for res in results -%}
{%- set run_data = {'invocation_id': invocation_id, 'node_id': res.node.unique_id, 'node_name': res.node.name, 'resource_type': res.node.resource_type, 'status': res.status, 'message': res.message | replace('"', "'"), 'rows_affected': res.adapter_response.get('rows_affected', 0), 'execution_time_seconds': res.execution_time, 'run_started_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S'), 'run_completed_at': modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -%}{%- do runs.append(run_data) -%}
{%- endfor -%}
{%- endif -%}
{%- if runs|length > 0 -%}
{%- set insert_query -%}
insert into {{ target_table }} (invocation_id, node_id, node_name, resource_type, status, message, rows_affected, execution_time_seconds, run_started_at, run_completed_at, extracted_at) values
{%- for r in runs -%}
('{{ r.invocation_id }}', '{{ r.node_id }}', '{{ r.node_name }}', '{{ r.resource_type }}', '{{ r.status }}', '{{ r.message | replace("'", "''") }}', {{ r.rows_affected | default(0) }}, {{ r.execution_time_seconds | default(0) }}, '{{ r.run_started_at }}'::timestamp, '{{ r.run_completed_at }}'::timestamp, now()){{ "," if not loop.last }}
{%- endfor -%}
{%- endset -%}{%- do run_query(insert_query) -%}{%- do log("Uploaded run results to " ~ target_table, info=True) -%}
{%- endif -%}{%- endif -%}
{%- endmacro -%}

{%- macro upload_test_definitions() -%}
{%- set target_table = get_artifact_schema() ~ ".test_artifacts__current" -%}
{%- set relation = adapter.get_relation(database=target.database, schema=target.schema, identifier='test_artifacts__current') -%}
{%- if relation is not none -%}
{%- do run_query("truncate table " ~ target_table) -%}{%- set tests = [] -%}
{%- if execute -%}
{%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "test") -%}
{%- set is_generic = node.test_metadata is defined and node.test_metadata -%}
{%- set test_data = {'invocation_id': invocation_id, 'node_id': node.unique_id, 'test_name': node.name, 'test_type': 'generic' if is_generic else 'singular', 'description': (node.description | default('') | replace('"', "'")), 'tags': tojson(node.tags | default([])), 'test_metadata': tojson(node.test_metadata | default({})), 'target_model_ids': tojson(node.depends_on.nodes | default([]))} -%}{%- do tests.append(test_data) -%}
{%- endfor -%}
{%- endif -%}
{%- if tests|length > 0 -%}
{%- set insert_query -%}
insert into {{ target_table }} (invocation_id, node_id, test_name, test_type, description, tags, test_metadata, target_model_ids, extracted_at) values
{%- for t in tests -%}
('{{ t.invocation_id }}', '{{ t.node_id }}', '{{ t.test_name }}', '{{ t.test_type }}', '{{ t.description | replace("'", "''") }}', '{{ t.tags }}', '{{ t.test_metadata | replace("'", "''") }}', '{{ t.target_model_ids }}', now()){{ "," if not loop.last }}
{%- endfor -%}
{%- endset -%}{%- do run_query(insert_query) -%}{%- do log("Uploaded test definitions to " ~ target_table, info=True) -%}
{%- endif -%}{%- endif -%}
{%- endmacro -%}