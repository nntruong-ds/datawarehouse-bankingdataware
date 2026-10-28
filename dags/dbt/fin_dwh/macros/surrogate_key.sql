{# Khóa thay thế (surrogate key) kiểu Kimball: md5 của các cột tự nhiên. #}
{% macro surrogate_key(cols) -%}
    md5(concat_ws('||', {% for c in cols %}coalesce(cast({{ c }} as text), '∅'){% if not loop.last %}, {% endif %}{% endfor %}))
{%- endmacro %}
