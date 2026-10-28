# Enterprise Financial Data Warehouse — Automated Governance & Compliance Platform

Kho dữ liệu ngân hàng giả lập chạy local: **Postgres 16 · dbt · Airflow 3 + Astronomer Cosmos · Elementary · SQLFluff · GitHub Actions**.

| Điểm chính | Ở đâu trong repo |
| --- | --- |
| Kimball Star Schema (dim/fact) | `dags/dbt/fin_dwh/models/marts/` |
| Incremental merge + quét lùi `lookback_days` (giao dịch liên ngân hàng về trễ) | `models/marts/fct_transactions.sql` |
| SCD Type 2 lịch sử nhóm nợ (NPL) bằng dbt snapshot | `snapshots/snap_loans.yml`, `models/marts/dim_loan_history.sql` |
| Circuit breaker: sổ cái lệch Nợ/Có → dừng báo cáo | `tests/assert_trial_balance.sql` |
| Post-hook đọc `meta: {pii: true}` → RLS theo chi nhánh + che PII (HMAC-SHA256) | `macros/governance/apply_governance.sql`, `warehouse/init/01_init.sql` |
| Điều phối: mỗi model là 1 task, test chạy ngay sau model | `dags/fin_dwh_daily.py` |
| Dữ liệu giả lập + nạp raw | `include/data_generator/bankgen.py` |

## Chạy nhanh

```bash
# 1. Warehouse
docker compose -f docker-compose.override.yml up -d

# 2. Công cụ
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt

# 3. Dữ liệu
python include/data_generator/bankgen.py init --days 30
python include/data_generator/bankgen.py load

# 4. dbt
cd dags/dbt/fin_dwh && dbt deps --profiles-dir . && dbt build --profiles-dir .
```

Tài khoản demo (đổi khi dùng thật): `dbt_runner / dbt_runner_pw`, `analyst_hn`, `analyst_hcm / analyst_pw`, `auditor_hq / auditor_pw`.
File `.env` chỉ chứa mật khẩu demo; dự án thật không commit file này.
