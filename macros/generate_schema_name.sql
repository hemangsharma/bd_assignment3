{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}  # Use target schema if custom schema name is not provided
    {%- else -%}
        {{ custom_schema_name | trim }}  # Use custom schema name directly
    {%- endif -%}
{%- endmacro %}