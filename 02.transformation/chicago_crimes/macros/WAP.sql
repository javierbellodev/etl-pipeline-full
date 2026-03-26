{% macro publish() %}
    {% set tables = [
        'fact_chicago_crimes',
        'dim_crime_type',
        'dim_location',
        'dim_date'
    ] %}

    {%- if target.name == 'green' -%}
        {% do log("Publicando tablas de blue → green (WAP)...", info=True) %}
        {% do run_query('CREATE SCHEMA IF NOT EXISTS green') %}

        {% for table_name in tables %}
            {% set sql %}
                CREATE OR REPLACE TABLE green.{{ table_name }}
                AS SELECT * FROM blue.{{ table_name }};
            {% endset %}
            {% do run_query(sql) %}
            {% do log("  ✔ green." ~ table_name ~ " publicada", info=True) %}
        {% endfor %}

        {% do log("Publicación WAP completada: blue → green", info=True) %}
    {%- else -%}
        {% do exceptions.warn("[WARNING]: El target debe ser 'green' para publicar. Target actual: '" ~ target.name ~ "'") %}
    {%- endif -%}
{% endmacro %}
