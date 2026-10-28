{#
  Mặc định dbt ghép tên schema thành "dbt_marts". Macro này dùng đúng tên
  khai báo (+schema: marts -> schema "marts") cho dễ đọc khi demo.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
