{#
  Kiểm chứng logic point-in-time của rpt_npl_ratio_daily bằng dữ liệu NGUỒN
  (raw.loans), không qua snapshot hay dim_loan_history, để không tự xác nhận
  chính logic cần kiểm tra:

  1. Đủ khoản vay: mỗi ngày được công bố phải đếm đúng số khoản vay đã giải
     ngân tới ngày đó, không thiếu (lịch sử hổng) và không thừa (một khoản vay
     bị tính hai phiên bản).
  2. Khớp hiện tại: ngày mới nhất của báo cáo phải khớp từng chi nhánh với trạng
     thái hiện tại của nguồn (số khoản vay, dư nợ xấu).
  3. Không bị tụt lại: báo cáo không được dừng ở ngày cũ hơn lần đổi nhóm nợ
     mới nhất của nguồn (vd khi bảng fact sổ cái ngừng nạp).
#}
{{ config(severity='error') }}

with rpt as (
    select
        as_of_date,
        branch_id,
        loan_count,
        npl_principal
    from {{ ref('rpt_npl_ratio_daily') }}
),

src as (
    select
        loan_id,
        branch_id,
        principal,
        disbursed_date,
        debt_group >= 3 as is_npl,
        updated_at
    from {{ source('core', 'loans') }}
),

last_day as (
    select max(as_of_date) as as_of_date from rpt
),

day_totals as (
    select
        as_of_date,
        sum(loan_count) as loan_count
    from rpt
    group by 1
),

check_complete as (
    select
        'thiếu/thừa khoản vay' as check_name,
        t.as_of_date,
        null::text as branch_id,
        t.loan_count as rpt_value,
        (
            select count(*) from src as s
            where s.disbursed_date <= t.as_of_date
        ) as src_value
    from day_totals as t
),

src_current as (
    select
        branch_id,
        count(*) as loan_count,
        sum(case when is_npl then principal else 0 end) as npl_principal
    from src
    group by 1
),

rpt_last as (
    select
        r.branch_id,
        r.loan_count,
        r.npl_principal
    from rpt as r
    inner join last_day as l
        on r.as_of_date = l.as_of_date
),

check_current as (
    select
        'lệch trạng thái hiện tại' as check_name,
        (select l.as_of_date from last_day as l) as as_of_date,
        coalesce(s.branch_id, r.branch_id) as branch_id,
        r.npl_principal as rpt_value,
        s.npl_principal as src_value
    from src_current as s
    full outer join rpt_last as r
        on s.branch_id = r.branch_id
    where
        s.loan_count is distinct from r.loan_count
        or s.npl_principal is distinct from r.npl_principal
),

check_lag as (
    select
        'báo cáo tụt lại sau nguồn' as check_name,
        l.as_of_date,
        null::text as branch_id,
        null::numeric as rpt_value,
        null::numeric as src_value
    from last_day as l
    where l.as_of_date is null or l.as_of_date < (select max(s.updated_at)::date from src as s)
)

select * from check_complete
where rpt_value <> src_value
union all
select * from check_current
union all
select * from check_lag
