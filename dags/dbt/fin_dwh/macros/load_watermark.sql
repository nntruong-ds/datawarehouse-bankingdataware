{#
  Mốc incremental của các bảng fact: thời điểm warehouse nhận dòng mới nhất đã vào
  bảng (cột loaded_at, lấy từ raw._loaded_at), trừ biên an toàn load_lookback_minutes.
  Bảng tạo từ bản cũ chưa có cột loaded_at -> trả về -infinity để lần chạy đó xử lý
  lại toàn bộ một lần (on_schema_change='append_new_columns' sẽ thêm cột vào bảng).
#}
{% macro load_watermark() -%}
    {%- set cols = adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list -%}
    {%- if 'loaded_at' in cols -%}
        (select coalesce(max(prev_run.loaded_at), '-infinity'::timestamptz) from {{ this }} as prev_run)
    {%- else -%}
        '-infinity'::timestamptz
    {%- endif %} - interval '{{ var('load_lookback_minutes') | int }} minutes'
{%- endmacro %}
