{#
  Tỷ lệ nợ xấu (NPL = nhóm 3-5) theo ngày và chi nhánh, tính "as-of" từ
  dimension SCD2: mỗi ngày lấy phiên bản khoản vay còn hiệu lực cuối ngày đó.

  Snapshot chỉ có lịch sử TỪ LẦN CHẠY ĐẦU TIÊN. Khoản vay đổi nhóm nợ trước lần
  đó chỉ còn phiên bản cuối, nên những ngày trước nó "vắng mặt"; vì nợ chủ yếu
  chuyển xấu đi, các ngày đó sẽ cho tỷ lệ NPL thấp hơn thực tế. Vì vậy chỉ công
  bố ngày ĐỦ LỊCH SỬ: mọi khoản vay đã giải ngân tới ngày đó đều có đúng một phiên
  bản hiệu lực. Test assert_npl_point_in_time kiểm chứng lại với dữ liệu nguồn.
#}
with days as (
    select generate_series(
        (select min(valid_from)::date from {{ ref('dim_loan_history') }}),
        -- ngày nghiệp vụ mới nhất = ngày hạch toán mới nhất trong sổ cái
        (select max(posting_date) from {{ ref('fct_gl_entries') }}),
        interval '1 day'
    )::date as as_of_date
),

as_of as (
    select
        d.as_of_date,
        l.loan_id,
        l.branch_id,
        l.principal,
        l.is_npl
    from days as d
    inner join {{ ref('dim_loan_history') }} as l
        on
            d.as_of_date >= l.valid_from::date
            and d.as_of_date < l.valid_to::date
),

loans as (
    select distinct
        loan_id,
        disbursed_date
    from {{ ref('dim_loan_history') }}
),

complete_days as (
    select d.as_of_date
    from days as d
    where
        (
            select count(*) from as_of as a
            where a.as_of_date = d.as_of_date
        )
        = (
            select count(*) from loans as ln
            where ln.disbursed_date <= d.as_of_date
        )
)

select
    a.as_of_date,
    a.branch_id,
    count(*) as loan_count,
    sum(a.principal) as total_principal,
    sum(case when a.is_npl then a.principal else 0 end) as npl_principal,
    round(
        100.0 * sum(case when a.is_npl then a.principal else 0 end) / nullif(sum(a.principal), 0),
        2
    ) as npl_ratio_pct
from as_of as a
inner join complete_days as c
    on a.as_of_date = c.as_of_date
group by 1, 2
