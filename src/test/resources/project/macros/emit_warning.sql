{% macro emit_warning_log() %}
    {% do exceptions.warn("This is a test warning from dbt") %}
{% endmacro %}
