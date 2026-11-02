{#
  Đối soát sổ cái staging ↔ fact: theo từng ngày hạch toán × chi nhánh × loại tiền,
  fact phải có đúng số dòng và đúng tổng Nợ/Có như nguồn.
  Lệch nghĩa là fct_gl_entries nạp thiếu (hoặc thừa) bút toán, và mọi báo cáo
  đọc từ fact đang sai dù sổ cái nguồn vẫn cân -> mức error.
#}
{{ config(severity='error') }}

with src as (
    select
        posting_date,
        branch_id,
        currency,
        count(*) as entry_count,
        sum(debit_amount) as total_debit,
        sum(credit_amount) as total_credit
    from {{ ref('stg_core__gl_entries') }}
    group by 1, 2, 3
),

fct as (
    select
        posting_date,
        branch_id,
        currency,
        count(*) as entry_count,
        sum(debit_amount) as total_debit,
        sum(credit_amount) as total_credit
    from {{ ref('fct_gl_entries') }}
    group by 1, 2, 3
)

select
    coalesce(src.posting_date, fct.posting_date) as posting_date,
    coalesce(src.branch_id, fct.branch_id) as branch_id,
    coalesce(src.currency, fct.currency) as currency,
    src.entry_count as src_entries,
    fct.entry_count as fct_entries,
    src.total_debit - fct.total_debit as debit_diff,
    src.total_credit - fct.total_credit as credit_diff
from src
full outer join fct
    on
        src.posting_date = fct.posting_date
        and src.branch_id = fct.branch_id
        and src.currency = fct.currency
where
    src.entry_count is distinct from fct.entry_count
    or src.total_debit is distinct from fct.total_debit
    or src.total_credit is distinct from fct.total_credit
