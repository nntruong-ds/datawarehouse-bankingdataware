{#
  Tỷ lệ nợ xấu (NPL = nhóm 3-5) theo ngày và chi nhánh, tính "as-of" từ
  dimension SCD2: mỗi ngày lấy phiên bản khoản vay còn hiệu lực ngày đó.
  Lưu ý: snapshot chỉ có lịch sử TỪ LẦN CHẠY ĐẦU TIÊN, không có quá khứ trước đó.
#}
with days as (
    select generate_series(
        (select min(valid_from)::date from {{ ref('dim_loan_history') }}),
        -- ngày nghiệp vụ mới nhất = ngày hạch toán mới nhất trong sổ cái
        (select max(posting_date) from {{ ref('fct_gl_entries') }}),
        interval '1 day'
    )::date as as_of_date
)

select
    d.as_of_date,
    l.branch_id,
    count(*) as loan_count,
    sum(l.principal) as total_principal,
    sum(case when l.is_npl then l.principal else 0 end) as npl_principal,
    round(
        100.0 * sum(case when l.is_npl then l.principal else 0 end) / nullif(sum(l.principal), 0),
        2
    ) as npl_ratio_pct
from days as d
inner join {{ ref('dim_loan_history') }} as l
    on
        d.as_of_date >= l.valid_from::date
        and d.as_of_date < l.valid_to::date
group by 1, 2
