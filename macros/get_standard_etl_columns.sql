{% macro get_standard_etl_columns() %}

    {{- '"' + var('PROCESS_BATCH_ID', invocation_id) + '"' + ' BATCH_ID_CREATED, ' -}}
    {{- '"' + var('PROCESS_BATCH_ID', invocation_id) + '"'  + ' BATCH_ID_UPDATED, ' -}}
    {{- 'CURRENT_TIMESTAMP CREATED_TIMESTAMP, '-}}
    {{- 'CURRENT_TIMESTAMP UPDATED_TIMESTAMP ' -}}

{% endmacro %}