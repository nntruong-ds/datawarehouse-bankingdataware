"""
bankgen.py — Sinh dữ liệu ngân hàng giả lập và nạp vào Postgres (schema raw).

Mô phỏng đúng 3 "vấn đề thật" mà dự án cần xử lý:
  1. Giao dịch liên ngân hàng (NAPAS) về TRỄ 1-5 ngày, và đổi trạng thái
     PENDING -> SETTLED/REVERSED sau khi đã nạp.
  2. Khoản vay CHUYỂN NHÓM NỢ theo thời gian (nhóm 1-5; nhóm 3-5 là nợ xấu/NPL).
  3. Bút toán sổ cái (GL) luôn cân Nợ = Có, trừ khi bật --inject-imbalance để
     thử cơ chế ngắt mạch.

Dữ liệu được ghi ra thư mục landing theo "ngày về" (ingest date), giống file
mà hệ thống nguồn đẩy sang mỗi ngày. Lệnh `load` nạp các file đến hạn vào DB.

Cách dùng:
  python bankgen.py init --start 2026-09-01 --days 30   # sinh lịch sử 30 ngày
  python bankgen.py load                                 # nạp file đến hạn
  python bankgen.py next-day                             # sinh thêm 1 ngày
  python bankgen.py next-day --inject-imbalance          # cố tình làm lệch sổ cái
  python bankgen.py reset-db                             # xóa sạch để làm lại từ đầu
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

DATA_DIR = Path(os.environ.get("BANKGEN_DATA_DIR", Path(__file__).parent / "data"))
LANDING = DATA_DIR / "landing"
STATE_FILE = DATA_DIR / "state.json"
SEED = 42

BRANCHES = [
    ("HN01", "Chi nhánh Hoàn Kiếm", "Hà Nội"),
    ("HN02", "Chi nhánh Cầu Giấy", "Hà Nội"),
    ("HCM01", "Chi nhánh Quận 1", "TP.HCM"),
    ("HCM02", "Chi nhánh Thủ Đức", "TP.HCM"),
    ("DN01", "Chi nhánh Hải Châu", "Đà Nẵng"),
]
HO = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng", "Bùi", "Đỗ"]
DEM = ["Văn", "Thị", "Minh", "Đức", "Thu", "Ngọc", "Quang", "Thanh", "Hữu", "Gia"]
TEN = ["An", "Bình", "Châu", "Dũng", "Giang", "Hà", "Hùng", "Lan", "Linh", "Long", "Mai",
       "Nam", "Phương", "Quân", "Sơn", "Tâm", "Trang", "Tuấn", "Vy", "Yến"]
BANKS = ["VCB", "BIDV", "CTG", "TCB", "MBB", "ACB", "VPB", "TPB"]

# Tài khoản sổ cái — MÃ MINH HỌA, không phải hệ thống tài khoản chính thức.
GL_CASH = "1011"           # Tiền mặt tại đơn vị
GL_DEPOSIT = "4211"        # Tiền gửi không kỳ hạn của khách hàng
GL_CLEARING = "5012"       # Thanh toán bù trừ liên ngân hàng
GL_FEE_INCOME = "7110"     # Thu phí dịch vụ thanh toán

# Độ trễ của giao dịch NAPAS (ngày) và xác suất: phần lớn về trong ngày,
# 1% về trễ 5 ngày (mô phỏng nghỉ lễ) — vượt cửa sổ quét lùi mặc định.
NAPAS_DELAY = [(0, 0.75), (1, 0.15), (2, 0.06), (3, 0.03), (5, 0.01)]

TXN_COLS = ["txn_id", "event_seq", "account_id", "branch_id", "txn_ts", "txn_date", "channel",
            "direction", "amount", "currency", "counterparty_bank", "status", "ingested_at"]
GL_COLS = ["entry_id", "journal_id", "posting_date", "branch_id", "gl_account", "currency",
           "debit_amount", "credit_amount", "source_txn_id", "ingested_at"]
LOAN_COLS = ["loan_id", "customer_id", "branch_id", "principal", "currency", "disbursed_date",
             "debt_group", "classified_date", "updated_at"]


# ----------------------------------------------------------------- tiện ích
def _state() -> dict:
    return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}


def _save_state(s: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(s, indent=2, ensure_ascii=False))


def _write_csv(path: Path, cols: list[str], rows: list[dict], append: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists() or not append
    with path.open("a" if append else "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if new_file:
            w.writeheader()
        w.writerows(rows)


def _read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _pick_delay(rng: random.Random) -> int:
    r, acc = rng.random(), 0.0
    for d, p in NAPAS_DELAY:
        acc += p
        if r <= acc:
            return d
    return 0


# ------------------------------------------------------------ dữ liệu master
def _gen_master(rng: random.Random, start: date) -> None:
    customers, accounts = [], []
    for i in range(1, 2001):
        b = rng.choice(BRANCHES)[0]
        cid = f"C{i:06d}"
        customers.append({
            "customer_id": cid,
            "full_name": f"{rng.choice(HO)} {rng.choice(DEM)} {rng.choice(TEN)}",
            "national_id": f"0{rng.randint(1, 96):02d}{rng.randint(0, 1)}{rng.randint(0, 99999999):08d}",
            "phone": f"0{rng.choice(['9', '3', '8'])}{rng.randint(0, 99999999):08d}",
            "email": f"kh{i:06d}@example.vn",
            "date_of_birth": (date(1960, 1, 1) + timedelta(days=rng.randint(0, 15000))).isoformat(),
            "segment": rng.choices(["RETAIL", "PRIORITY", "SME"], [0.8, 0.1, 0.1])[0],
            "home_branch_id": b,
            "created_at": (start - timedelta(days=rng.randint(30, 2000))).isoformat(),
        })
        for _ in range(rng.choices([1, 2], [0.7, 0.3])[0]):
            accounts.append({
                "account_id": f"A{len(accounts) + 1:07d}",
                "customer_id": cid,
                "branch_id": b,
                "product_type": rng.choices(["CASA", "SAVING"], [0.8, 0.2])[0],
                "currency": rng.choices(["VND", "USD"], [0.9, 0.1])[0],
                "opened_date": (start - timedelta(days=rng.randint(10, 1500))).isoformat(),
                "status": "ACTIVE",
            })

    loans = []
    for i in range(1, 801):
        c = rng.choice(customers)
        g = rng.choices([1, 2, 3, 4, 5], [0.88, 0.07, 0.02, 0.015, 0.015])[0]
        disb = start - timedelta(days=rng.randint(60, 1500))
        loans.append({
            "loan_id": f"L{i:06d}", "customer_id": c["customer_id"], "branch_id": c["home_branch_id"],
            "principal": rng.randrange(50, 5000) * 1_000_000, "currency": "VND",
            "disbursed_date": disb.isoformat(), "debt_group": g,
            "classified_date": (start - timedelta(days=1)).isoformat(),
            "updated_at": datetime.combine(start - timedelta(days=1), datetime.min.time()).isoformat(),
        })

    master = LANDING / "master"
    _write_csv(master / "branches.csv", ["branch_id", "branch_name", "region"],
               [dict(zip(["branch_id", "branch_name", "region"], b)) for b in BRANCHES])
    _write_csv(master / "customers.csv", list(customers[0].keys()), customers)
    _write_csv(master / "accounts.csv", list(accounts[0].keys()), accounts)
    _write_csv(LANDING / "loans_current.csv", LOAN_COLS, loans)


# ------------------------------------------------------------ một ngày giao dịch
def _gen_day(rng: random.Random, d: date, inject_imbalance: bool = False) -> None:
    accounts = _read_csv(LANDING / "master" / "accounts.csv")
    n = rng.randint(2500, 3500)
    by_ingest: dict[date, tuple[list, list]] = {}

    def bucket(day: date):
        return by_ingest.setdefault(day, ([], []))

    for _ in range(n):
        acc = rng.choice(accounts)
        ts = datetime.combine(d, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86399))
        channel = rng.choices(["INTERNAL", "NAPAS", "ATM"], [0.45, 0.4, 0.15])[0]
        direction = rng.choice(["IN", "OUT"])
        cur = acc["currency"]
        amount = (rng.randrange(1, 2000) * 10_000) if cur == "VND" else round(rng.uniform(10, 5000), 2)
        txn_id = str(uuid.UUID(int=rng.getrandbits(128)))
        delay = _pick_delay(rng) if channel == "NAPAS" else 0
        ingest_day = d + timedelta(days=delay)
        ingested_at = datetime.combine(ingest_day, datetime.min.time()) + timedelta(hours=23)
        status = "PENDING" if channel == "NAPAS" else "SETTLED"

        base = {"txn_id": txn_id, "account_id": acc["account_id"], "branch_id": acc["branch_id"],
                "txn_ts": ts.isoformat(), "txn_date": d.isoformat(), "channel": channel,
                "direction": direction, "amount": amount, "currency": cur,
                "counterparty_bank": rng.choice(BANKS) if channel == "NAPAS" else "",
                }
        bucket(ingest_day)[0].append({**base, "event_seq": 1, "status": status,
                                       "ingested_at": ingested_at.isoformat()})

        # NAPAS: hôm sau mới có kết quả quyết toán -> sự kiện thứ 2 cùng txn_id
        if channel == "NAPAS":
            settle_day = ingest_day + timedelta(days=1)
            final = "REVERSED" if rng.random() < 0.02 else "SETTLED"
            bucket(settle_day)[0].append({
                **base, "event_seq": 2, "status": final,
                "ingested_at": (datetime.combine(settle_day, datetime.min.time())
                                + timedelta(hours=23)).isoformat()})

        # Bút toán kép: tiền vào -> Nợ quỹ/bù trừ, Có tiền gửi KH; tiền ra ngược lại
        other = {"INTERNAL": GL_DEPOSIT, "NAPAS": GL_CLEARING, "ATM": GL_CASH}[channel]
        dr, cr = (other, GL_DEPOSIT) if direction == "IN" else (GL_DEPOSIT, other)
        if channel == "INTERNAL":
            dr, cr = GL_DEPOSIT, GL_DEPOSIT  # chuyển nội bộ giữa 2 TK khách hàng
        lines = [(dr, amount, 0), (cr, 0, amount)]
        if channel == "NAPAS" and direction == "OUT":  # phí chuyển liên NH
            fee = 5_500 if cur == "VND" else 1
            lines += [(GL_DEPOSIT, fee, 0), (GL_FEE_INCOME, 0, fee)]
        for k, (gl, de, cr_amt) in enumerate(lines, 1):
            bucket(ingest_day)[1].append({
                "entry_id": f"{txn_id}-{k}", "journal_id": txn_id,
                "posting_date": ingest_day.isoformat(), "branch_id": acc["branch_id"],
                "gl_account": gl, "currency": cur, "debit_amount": de, "credit_amount": cr_amt,
                "source_txn_id": txn_id, "ingested_at": ingested_at.isoformat()})

    for ingest_day, (txns, gls) in by_ingest.items():
        if inject_imbalance and ingest_day == d and gls:
            # Xóa 1 dòng Có -> sổ cái chi nhánh đó lệch, test phải chặn pipeline
            idx = next(i for i, g in enumerate(gls) if float(g["credit_amount"]) > 0)
            removed = gls.pop(idx)
            print(f"[inject-imbalance] Đã xóa dòng Có {removed['entry_id']} "
                  f"({removed['branch_id']}, {removed['currency']}, {removed['credit_amount']})")
        tag = ingest_day.isoformat()
        _write_csv(LANDING / "transactions" / f"ingest_date={tag}.csv", TXN_COLS, txns, append=True)
        _write_csv(LANDING / "gl_entries" / f"ingest_date={tag}.csv", GL_COLS, gls, append=True)


def _migrate_loans(rng: random.Random, d: date) -> int:
    """Mỗi ngày ~1% khoản vay chuyển nhóm nợ (xấu đi nhiều hơn tốt lên)."""
    path = LANDING / "loans_current.csv"
    loans = _read_csv(path)
    changed = 0
    for ln in loans:
        if rng.random() < 0.01:
            g = int(ln["debt_group"])
            g = min(5, g + 1) if rng.random() < 0.7 else max(1, g - 1)
            if g != int(ln["debt_group"]):
                ln["debt_group"] = g
                ln["classified_date"] = d.isoformat()
                ln["updated_at"] = (datetime.combine(d, datetime.min.time()) + timedelta(hours=22)).isoformat()
                changed += 1
    _write_csv(path, LOAN_COLS, loans)
    return changed


# ------------------------------------------------------------------- lệnh
def cmd_init(args) -> None:
    if STATE_FILE.exists() and not args.force:
        raise SystemExit("Đã có dữ liệu. Thêm --force để sinh lại từ đầu.")
    if LANDING.exists():
        import shutil
        shutil.rmtree(LANDING)
    rng = random.Random(SEED)
    start = date.fromisoformat(args.start)
    _gen_master(rng, start)
    for i in range(args.days):
        d = start + timedelta(days=i)
        _gen_day(rng, d)
        _migrate_loans(rng, d)
    _save_state({"start": start.isoformat(), "current_date": (start + timedelta(days=args.days - 1)).isoformat(),
                 "rng_state_seed": SEED + args.days})
    print(f"Đã sinh {args.days} ngày từ {start}. Ngày hiện tại mô phỏng: {_state()['current_date']}")


def cmd_next_day(args) -> None:
    s = _state()
    if not s:
        raise SystemExit("Chưa init. Chạy: python bankgen.py init")
    d = date.fromisoformat(s["current_date"]) + timedelta(days=1)
    rng = random.Random(s["rng_state_seed"])
    _gen_day(rng, d, inject_imbalance=args.inject_imbalance)
    changed = _migrate_loans(rng, d)
    s["current_date"] = d.isoformat()
    s["rng_state_seed"] += 1
    _save_state(s)
    print(f"Đã sinh ngày {d}; {changed} khoản vay đổi nhóm nợ.")


def _connect():
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("DWH_HOST", "localhost"), port=int(os.environ.get("DWH_PORT", "5433")),
        dbname=os.environ.get("DWH_DB", "dwh"), user=os.environ.get("DWH_USER", "dbt_runner"),
        password=os.environ.get("DWH_PASSWORD", "dbt_runner_pw"))


DDL = """
create table if not exists raw.branches (branch_id text, branch_name text, region text);
create table if not exists raw.customers (customer_id text, full_name text, national_id text, phone text,
    email text, date_of_birth date, segment text, home_branch_id text, created_at date);
create table if not exists raw.accounts (account_id text, customer_id text, branch_id text, product_type text,
    currency text, opened_date date, status text);
create table if not exists raw.transactions (txn_id text, event_seq int, account_id text, branch_id text,
    txn_ts timestamp, txn_date date, channel text, direction text, amount numeric(20,2), currency text,
    counterparty_bank text, status text, ingested_at timestamp);
create table if not exists raw.gl_entries (entry_id text, journal_id text, posting_date date, branch_id text,
    gl_account text, currency text, debit_amount numeric(20,2), credit_amount numeric(20,2),
    source_txn_id text, ingested_at timestamp);
create table if not exists raw.loans (loan_id text, customer_id text, branch_id text, principal numeric(20,2),
    currency text, disbursed_date date, debt_group int, classified_date date, updated_at timestamp);
create table if not exists raw._load_log (dataset text, ingest_date date, rows_loaded int,
    loaded_at timestamp default now(), primary key (dataset, ingest_date));
"""


def _copy(cur, table: str, path: Path) -> int:
    with path.open(encoding="utf-8") as f:
        cols = f.readline().strip()
        cur.copy_expert(f"copy {table} ({cols}) from stdin with (format csv, null '')", f)
    return cur.rowcount


def cmd_load(args) -> None:
    s = _state()
    if not s:
        raise SystemExit("Chưa init. Chạy: python bankgen.py init")
    current = date.fromisoformat(s["current_date"])
    conn = _connect()
    conn.autocommit = False
    with conn, conn.cursor() as cur:
        cur.execute(DDL)
        # Master + khoản vay: ảnh chụp toàn bộ -> thay mới mỗi lần nạp
        for t in ["branches", "customers", "accounts"]:
            cur.execute(f"truncate raw.{t}")
            _copy(cur, f"raw.{t}", LANDING / "master" / f"{t}.csv")
        cur.execute("truncate raw.loans")
        n_loans = _copy(cur, "raw.loans", LANDING / "loans_current.csv")
        # Giao dịch + sổ cái: chỉ nạp file đến hạn (ingest_date <= ngày hiện tại) và chưa nạp
        cur.execute("select dataset, ingest_date from raw._load_log")
        done = {(r[0], r[1]) for r in cur.fetchall()}
        loaded = 0
        for ds, table in [("transactions", "raw.transactions"), ("gl_entries", "raw.gl_entries")]:
            for f in sorted((LANDING / ds).glob("ingest_date=*.csv")):
                day = date.fromisoformat(f.stem.split("=")[1])
                if day > current or (ds, day) in done:
                    continue
                n = _copy(cur, table, f)
                cur.execute("insert into raw._load_log (dataset, ingest_date, rows_loaded) values (%s,%s,%s)",
                            (ds, day, n))
                loaded += 1
    conn.close()
    print(f"Nạp xong: {loaded} file giao dịch/sổ cái mới, {n_loans} khoản vay (đến ngày {current}).")


def cmd_reset_db(args) -> None:
    """Xóa sạch dữ liệu đã nạp và mọi thứ dbt đã build, để chạy lại từ đầu."""
    conn = _connect()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("truncate raw.branches, raw.customers, raw.accounts, raw.transactions, "
                    "raw.gl_entries, raw.loans, raw._load_log")
        cur.execute("drop schema if exists staging, marts, snapshots, elementary, dbt cascade")
        cur.execute("drop schema if exists secure cascade")
        cur.execute("create schema secure")
        cur.execute("grant usage on schema secure to analyst_base")
    conn.close()
    print("Đã xóa dữ liệu raw và các schema do dbt tạo. Tiếp theo: init --force, load, dbt build.")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("init"); a.add_argument("--start", default="2026-09-01")
    a.add_argument("--days", type=int, default=30); a.add_argument("--force", action="store_true")
    b = sub.add_parser("next-day"); b.add_argument("--inject-imbalance", action="store_true")
    sub.add_parser("load")
    sub.add_parser("reset-db")
    args = p.parse_args()
    {"init": cmd_init, "next-day": cmd_next_day, "load": cmd_load,
     "reset-db": cmd_reset_db}[args.cmd](args)


if __name__ == "__main__":
    main()
