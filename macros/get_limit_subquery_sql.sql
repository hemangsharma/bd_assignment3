--macros/get_limit_subquery_sql.sql

{% macro default__get_limit_subquery_sql(sql, limit) %}
    {{ sql }}
{% endmacro %}