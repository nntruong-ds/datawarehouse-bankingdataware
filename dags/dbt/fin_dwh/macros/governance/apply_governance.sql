{#
  apply_governance() — chạy như post-hook sau mỗi model trong marts.

  Việc nó làm:
  1. Nếu model có config.meta.rls_branch_column:
       bật Row-Level Security trên bảng marts và tạo policy
       "chỉ thấy chi nhánh mình được phân quyền" (governance.can_see_branch).
  2. Cột masking = hash: băm SẴN ngay lúc build (bằng quyền của pipeline) vào
     cột ẩn <cột>__hmac trên bảng marts. Người đọc không cần, và không có,
     quyền gọi governance.pii_hash: nếu gọi được, họ chỉ cần đoán một số CCCD
     rồi so với giá trị đã băm là biết đúng hay sai, khóa bí mật mất tác dụng.
  3. Luôn tạo view secure.<tên model> cho người đọc báo cáo:
       - cột có meta.pii = true được che theo meta.masking,
       - người có role pii_reader thấy dữ liệu gốc,
       - RLS của bảng gốc vẫn áp dụng qua view,
       - cột ẩn __hmac không lộ ra ngoài view.
  4. Cấp quyền SELECT trên view cho nhóm analyst_base.

  Người đọc KHÔNG có quyền vào schema marts, chỉ vào schema secure.
#}

{% macro _column_meta(col) -%}
    {#- dbt >= 1.10 khuyến nghị đặt meta trong config; hỗ trợ cả cách cũ -#}
    {%- set cfg = col.get('config') or {} -%}
    {%- set m = (cfg.get('meta') if cfg is mapping else none) or col.get('meta') or {} -%}
    {{ return(m) }}
{%- endmacro %}

{% macro _hmac_column(column_name) -%}
    {{ return(column_name | lower ~ '__hmac') }}
{%- endmacro %}

{% macro _mask_expression(column_name, masking) -%}
    {%- set clear = adapter.quote(column_name) -%}
    {%- if masking == 'hash' -%}
        case when pg_has_role(current_user, 'pii_reader', 'member')
             then {{ clear }}::text else {{ adapter.quote(_hmac_column(column_name)) }} end
    {%- elif masking == 'partial' -%}
        case when pg_has_role(current_user, 'pii_reader', 'member')
             then {{ clear }}::text else governance.pii_partial({{ clear }}::text) end
    {%- else -%}
        case when pg_has_role(current_user, 'pii_reader', 'member')
             then {{ clear }} else null end
    {%- endif -%}
{%- endmacro %}

{% macro apply_governance() %}
    {%- if not execute or model.resource_type != 'model' -%}
        {{ return('select 1') }}
    {%- endif -%}

    {%- set model_meta = model.config.get('meta', {}) or {} -%}
    {%- set rls_col = model_meta.get('rls_branch_column') -%}

    {#- Đọc meta pii từ YAML -#}
    {%- set pii = {} -%}
    {%- for name, col in model.columns.items() -%}
        {%- set m = _column_meta(col) -%}
        {%- if m.get('pii') -%}
            {%- do pii.update({name | lower: m.get('masking', 'hash')}) -%}
        {%- endif -%}
    {%- endfor -%}

    {#- Danh sách cột thực tế của bảng vừa build (bỏ cột ẩn __hmac của lần chạy trước) -#}
    {%- set actual_cols = adapter.get_columns_in_relation(this) -%}
    {%- set select_list = [] -%}
    {%- set hash_cols = [] -%}
    {%- for c in actual_cols -%}
        {%- if (c.name | lower).endswith('__hmac') -%}
            {#- cột ẩn: không đưa ra view -#}
        {%- elif c.name | lower in pii -%}
            {%- if pii[c.name | lower] == 'hash' -%}{%- do hash_cols.append(c.name) -%}{%- endif -%}
            {%- do select_list.append(_mask_expression(c.name, pii[c.name | lower]) ~ ' as ' ~ adapter.quote(c.name)) -%}
        {%- else -%}
            {%- do select_list.append(adapter.quote(c.name)) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- set secure_view = api.Relation.create(database=this.database, schema='secure', identifier=this.identifier, type='view') -%}

    {%- set sql -%}
    {%- for c in hash_cols %}
    {%- set h = adapter.quote(_hmac_column(c)) %}
    alter table {{ this }} add column if not exists {{ h }} text;
    update {{ this }} set {{ h }} = governance.pii_hash({{ adapter.quote(c) }}::text)
        where {{ h }} is distinct from governance.pii_hash({{ adapter.quote(c) }}::text);
    {%- endfor %}
    {%- if rls_col %}
    alter table {{ this }} enable row level security;
    alter table {{ this }} force row level security;
    drop policy if exists rls_branch on {{ this }};
    create policy rls_branch on {{ this }}
        using (governance.can_see_branch({{ adapter.quote(rls_col) }}::text));
    {%- endif %}
    drop view if exists {{ secure_view }};
    create view {{ secure_view }} with (security_barrier = true) as
    select
        {{ select_list | join(',\n        ') }}
    from {{ this }};
    comment on view {{ secure_view }} is 'Tự sinh bởi apply_governance(); PII: {{ pii | tojson }}; RLS: {{ rls_col or "none" }}';
    grant select on {{ secure_view }} to analyst_base;
    {%- endset -%}

    {{ log("[governance] " ~ this.identifier ~ " | RLS=" ~ (rls_col or "none") ~ " | PII=" ~ (pii | tojson), info=True) }}
    {{ return(sql) }}
{% endmacro %}
