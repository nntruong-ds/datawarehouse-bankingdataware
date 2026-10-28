select
    cast(to_char(d, 'YYYYMMDD') as integer) as date_key,
    d::date as full_date,
    extract(year from d)::int as calendar_year,
    extract(quarter from d)::int as calendar_quarter,
    extract(month from d)::int as calendar_month,
    extract(day from d)::int as day_of_month,
    extract(isodow from d)::int as day_of_week,
    extract(isodow from d) in (6, 7) as is_weekend,
    date_trunc('month', d)::date as month_start
from generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day') as d
