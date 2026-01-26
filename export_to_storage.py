import os
import io
import csv
import requests
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

BUCKET = os.environ.get("SUPABASE_BUCKET", "open-data")
BASE_PATH = os.environ.get("SUPABASE_BASE_PATH", "datasets")
RENTAL_DATE_COL = os.environ.get("RENTAL_DATE_COL", "rental_start_date")

HEADERS = {
    "apikey": SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
}

KST = ZoneInfo("Asia/Seoul")
RUNDATE = datetime.now(KST).strftime("%Y%m%d")


def fetch_all_rows(table: str, page_size: int = 2000, extra_params: dict | None = None):
    rows = []
    offset = 0
    url = f"{SUPABASE_URL}/rest/v1/{table}"

    while True:
        params = {"select": "*", "limit": str(page_size), "offset": str(offset)}
        if extra_params:
            params.update(extra_params)

        r = requests.get(url, headers=HEADERS, params=params, timeout=120)
        r.raise_for_status()
        batch = r.json()

        if not batch:
            break

        rows.extend(batch)
        offset += page_size

    return rows


def rows_to_csv_bytes(rows: list[dict]) -> bytes:
    if not rows:
        return b""
    fieldnames = list(rows[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def upload_to_storage(path_in_bucket: str, content: bytes, content_type: str = "text/csv; charset=utf-8"):
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{path_in_bucket}"
    headers = {**HEADERS, "Content-Type": content_type, "x-upsert": "true"}
    r = requests.post(url, headers=headers, data=content, timeout=300)
    r.raise_for_status()
    return r.json()


def seoul_yesterday_range():
    tz = ZoneInfo("Asia/Seoul")
    now = datetime.now(tz)
    target_date = now.date() - timedelta(days=1)
    start_dt = datetime.combine(target_date, time(0, 0, 0), tzinfo=tz)
    end_dt = start_dt + timedelta(days=1)
    return target_date.isoformat(), start_dt.isoformat(), end_dt.isoformat()


def export_overwrite(table: str, out_path: str):
    print(f"[EXPORT overwrite] {table} -> {BUCKET}/{out_path}")
    rows = fetch_all_rows(table=table)
    upload_to_storage(out_path, rows_to_csv_bytes(rows))
    print(f"[OK] rows={len(rows)} uploaded")


def main():
    overwrite_datasets = [
        ("users", f"{BASE_PATH}/users_{RUNDATE}.csv"),
        ("weekly_sales", f"{BASE_PATH}/weekly_sales_{RUNDATE}.csv"),
        ("hubs", f"{BASE_PATH}/hubs_{RUNDATE}.csv"),
        ("stations", f"{BASE_PATH}/stations_{RUNDATE}.csv"),
        ("bikes", f"{BASE_PATH}/bikes_{RUNDATE}.csv"),
        ("rental_logs", f"{BASE_PATH}/rental_logs_{RUNDATE}.csv"),
    ]

    for table, out_path in overwrite_datasets:
        export_overwrite(table, out_path)


if __name__ == "__main__":
    main()
