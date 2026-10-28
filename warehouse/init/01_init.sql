-- =====================================================================
-- Khởi tạo warehouse: role, schema, bảng quản trị, hàm băm HMAC.
-- Chạy MỘT LẦN khi container Postgres được tạo lần đầu
-- (docker-entrypoint-initdb.d tự chạy file này).
-- =====================================================================

-- 1) Extension để băm HMAC-SHA256
create extension if not exists pgcrypto;

-- 2) Role nhóm (không đăng nhập) ------------------------------------------
create role role_etl nologin;      -- tài khoản chạy pipeline (dbt, loader)
create role role_hq nologin;       -- hội sở: được xem mọi chi nhánh
create role pii_reader nologin;    -- được xem PII dạng gốc
create role analyst_base nologin;  -- quyền chung của mọi người đọc báo cáo

-- 3) Tài khoản đăng nhập (mật khẩu demo, đổi khi dùng thật) ----------------
-- QUAN TRỌNG: dbt_runner KHÔNG phải superuser. Superuser bỏ qua mọi RLS.
create role dbt_runner login password 'dbt_runner_pw' in role role_etl;
create role analyst_hn login password 'analyst_pw' in role analyst_base;   -- chỉ Hà Nội
create role analyst_hcm login password 'analyst_pw' in role analyst_base;  -- chỉ TP.HCM
create role auditor_hq login password 'auditor_pw' in role analyst_base, role_hq, pii_reader;

grant create, connect on database dwh to dbt_runner;

-- 4) Schema ----------------------------------------------------------------
create schema raw authorization dbt_runner;          -- dữ liệu thô từ nguồn
create schema governance authorization dbt_runner;   -- bảng & hàm quản trị
create schema secure authorization dbt_runner;       -- lớp view cho người đọc

-- staging / marts / snapshots do dbt tự tạo (dbt_runner có quyền CREATE).
-- Người đọc chỉ được vào schema secure:
grant usage on schema secure to analyst_base;
grant usage on schema governance to analyst_base;

-- 5) Bảng phân quyền chi nhánh: ai được xem chi nhánh nào ---------------
create table governance.user_branch_access (
    db_user   text not null,
    branch_id text not null,
    primary key (db_user, branch_id)
);
alter table governance.user_branch_access owner to dbt_runner;
insert into governance.user_branch_access values
    ('analyst_hn', 'HN01'), ('analyst_hn', 'HN02'),
    ('analyst_hcm', 'HCM01'), ('analyst_hcm', 'HCM02');
grant select on governance.user_branch_access to analyst_base;

-- Hàm dùng trong policy RLS. SECURITY INVOKER (mặc định) để current_user
-- là người đang truy vấn, không phải người tạo hàm.
create function governance.can_see_branch(p_branch_id text)
returns boolean
language sql stable
as $$
    select pg_has_role(current_user, 'role_etl', 'member')
        or pg_has_role(current_user, 'role_hq', 'member')
        or exists (
            select 1 from governance.user_branch_access a
            where a.db_user = current_user and a.branch_id = p_branch_id
        )
$$;
alter function governance.can_see_branch(text) owner to dbt_runner;
grant execute on function governance.can_see_branch(text) to analyst_base;

-- 6) Khóa bí mật cho HMAC ------------------------------------------------
-- Demo: để trong bảng không ai đọc được ngoài chủ sở hữu.
-- Thực tế: lấy từ Vault / Secret Manager.
create table governance.pii_key (k text not null);
alter table governance.pii_key owner to dbt_runner;
insert into governance.pii_key values (encode(gen_random_bytes(32), 'hex'));
revoke all on governance.pii_key from public;

-- Hàm băm: SECURITY DEFINER để người gọi dùng được khóa mà không đọc được khóa.
create function governance.pii_hash(p_value text)
returns text
language sql stable security definer
set search_path = governance, public
as $$
    select case when p_value is null then null
           else encode(hmac(p_value, (select k from governance.pii_key limit 1), 'sha256'), 'hex')
           end
$$;
alter function governance.pii_hash(text) owner to dbt_runner;
revoke all on function governance.pii_hash(text) from public;
grant execute on function governance.pii_hash(text) to analyst_base, role_etl;

-- Che một phần: giữ 4 ký tự cuối
create function governance.pii_partial(p_value text)
returns text
language sql immutable
as $$
    select case when p_value is null then null
           else repeat('*', greatest(length(p_value) - 4, 0)) || right(p_value, 4)
           end
$$;
alter function governance.pii_partial(text) owner to dbt_runner;
grant execute on function governance.pii_partial(text) to analyst_base, role_etl;
