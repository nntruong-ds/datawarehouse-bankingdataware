"""
DAG fin_dwh_daily — chạy mỗi sáng:

  1. generate_next_day : sinh dữ liệu ngày mới (giả lập hệ thống nguồn đẩy file)
  2. load_raw          : nạp file đến hạn vào schema raw
  3. dbt (Cosmos)      : mỗi snapshot/model dbt thành 1 task; test của model
                         chạy NGAY SAU model đó (TestBehavior.AFTER_EACH).
                         Test fail -> các task phía sau không chạy (circuit breaker).

Đặt trong thư mục dags/ của project Astro; dbt project nằm ở dags/dbt/fin_dwh.
"""
from __future__ import annotations

import os
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, chain, dag
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)
from pendulum import datetime

DAGS_DIR = Path(__file__).parent
DBT_PROJECT_PATH = Path(os.environ.get("DBT_PROJECT_PATH", DAGS_DIR / "dbt" / "fin_dwh"))
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")
BANKGEN = os.environ.get("BANKGEN_SCRIPT", "/usr/local/airflow/include/data_generator/bankgen.py")

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    # Cosmos tự chạy `dbt deps` (cài dbt_expectations, elementary) trước khi chạy dbt.
    # Khi project lớn: chạy deps sẵn trong CI rồi đặt False để nhanh hơn.
    install_dbt_deps=True,
)

profile_config = ProfileConfig(
    profile_name="fin_dwh",
    target_name="dev",
    # Dùng chính profiles.yml của project; host/port/mật khẩu đọc từ biến môi trường (.env)
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE,  # dbt cài trong venv riêng (xem Dockerfile)
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    test_behavior=TestBehavior.AFTER_EACH,
    # Test tham chiếu nhiều model (vd đối soát stg <-> fct) chạy SAU KHI mọi model
    # cha đã build xong, thay vì bị gắn vào model cha đầu tiên.
    should_detach_multiple_parents_tests=True,
    # Model nội bộ của Elementary không cần hiện thành task; build 1 lần bằng tay
    # (dbt run --select elementary). Kết quả test vẫn được Elementary ghi lại.
    exclude=["package:elementary"],
)


@dag(
    dag_id="fin_dwh_daily",
    schedule="15 6 * * *",  # 06:15 mỗi ngày (giờ UTC của Airflow)
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["dbt", "finance", "governance"],
    doc_md=__doc__,
    params={
        # Bật khi Trigger DAG để thử circuit breaker: sổ cái sẽ lệch, báo cáo không chạy
        "inject_imbalance": Param(False, type="boolean", description="Cố tình làm lệch sổ cái"),
    },
)
def fin_dwh_daily():
    generate_next_day = BashOperator(
        task_id="generate_next_day",
        bash_command=(
            f"python {BANKGEN} next-day"
            "{{ ' --inject-imbalance' if params.inject_imbalance else '' }}"
        ),
    )

    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=f"python {BANKGEN} load",
    )

    transform = DbtTaskGroup(
        group_id="dbt",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={"install_deps": True},
    )

    chain(generate_next_day, load_raw, transform)


fin_dwh_daily()
