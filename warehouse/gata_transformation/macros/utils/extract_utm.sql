{% macro extract_utm(url_column, param_name) %}
    {#
        Extracts a parameter value from a URL string using regex.
        Matches both '?param=value' and '&param=value'.
        DuckDB regexp_extract returns the captured group.
    #}
    regexp_extract({{ url_column }}, '[?&]{{ param_name }}=([^&]+)', 1)
{% endmacro %}
