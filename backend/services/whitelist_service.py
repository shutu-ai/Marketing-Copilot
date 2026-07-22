from database import get_connection
import pandas as pd
import os
from typing import List, Optional
import csv

UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "uploads")

TEMPLATE_COLUMNS = {"msisdn", "region", "language"}


def get_upload_history() -> list:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM upload_history ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _validate_rows(df) -> list:
    errors = []
    for idx, row in df.iterrows():
        msisdn = str(row.get("msisdn", "")).strip()
        if not msisdn:
            errors.append({"row": idx + 2, "error": "msisdn 为空"})
        elif not msisdn.isdigit():
            errors.append({"row": idx + 2, "error": f"msisdn 格式无效: {msisdn}"})
    return errors


def preview_file(file_path: str) -> dict:
    df = _read_file(file_path)
    if df is None:
        return {"headers": [], "rows": [], "total": 0}

    df.columns = [c.lower().strip() for c in df.columns]
    headers = list(df.columns)
    missing = TEMPLATE_COLUMNS - set(headers)

    rows = df.head(10).fillna("").to_dict(orient="records")
    result = {
        "headers": headers,
        "rows": [{k: str(v) for k, v in row.items()} for row in rows],
        "total": len(df),
        "missing_fields": list(missing),
    }

    format_errors = _validate_rows(df.head(10))
    if format_errors:
        result["format_errors"] = format_errors

    return result


def import_file(upload_id: int, file_path: str, import_mode: str = "append") -> dict:
    df = _read_file(file_path)
    if df is None:
        return {"upload_id": upload_id, "total": 0, "success": 0, "failed": 0, "errors": [{"row": 0, "error": "无法读取文件"}]}

    df.columns = [c.lower().strip() for c in df.columns]
    missing = TEMPLATE_COLUMNS - set(df.columns)
    if missing:
        return {"upload_id": upload_id, "total": 0, "success": 0, "failed": 0,
                "errors": [{"row": 0, "error": f"缺少字段: {', '.join(missing)}"}]}

    conn = get_connection()
    errors = []
    success_count = 0
    fail_count = 0

    if import_mode == "overwrite":
        conn.execute("DELETE FROM whitelist_users WHERE upload_id=?", (upload_id,))

    for idx, row in df.iterrows():
        msisdn = str(row.get("msisdn", "")).strip()
        if not msisdn:
            errors.append({"row": idx + 2, "error": "msisdn 为空"})
            fail_count += 1
            continue
        if not msisdn.isdigit():
            errors.append({"row": idx + 2, "error": f"msisdn 格式无效: {msisdn}"})
            fail_count += 1
            continue

        if import_mode == "skip" and conn.execute(
                "SELECT id FROM whitelist_users WHERE msisdn=? LIMIT 1", (msisdn,)).fetchone():
            continue

        try:
            conn.execute(
                """INSERT INTO whitelist_users (msisdn, region, language, status, upload_id)
                   VALUES (?,?,?,?,?)""",
                (msisdn,
                 str(row.get("region", "")).strip(),
                 str(row.get("language", "")).strip(),
                 str(row.get("status", "active")).strip(),
                 upload_id)
            )
            success_count += 1
        except Exception as e:
            errors.append({"row": idx + 2, "error": str(e)})
            fail_count += 1

    conn.execute(
        "UPDATE upload_history SET total_rows=?, success_rows=?, failed_rows=?, status=?, error_detail=? WHERE id=?",
        (success_count + fail_count, success_count, fail_count,
         "completed" if fail_count == 0 else "partial" if success_count > 0 else "failed",
         str(errors[:100]), upload_id)
    )
    conn.commit()
    conn.close()

    return {
        "upload_id": upload_id,
        "total": success_count + fail_count,
        "success": success_count,
        "failed": fail_count,
        "errors": errors[:100],
    }


def _read_file(file_path: str):
    try:
        ext = os.path.splitext(file_path)[1].lower()
        if ext in {".xlsx", ".xls"}:
            return pd.read_excel(file_path, dtype=str)
        with open(file_path, "r", encoding="utf-8-sig", newline="") as handle:
            sample = handle.read(8192)
        delimiter = csv.Sniffer().sniff(sample, delimiters=",|;\t").delimiter
        return pd.read_csv(file_path, dtype=str, sep=delimiter)
    except Exception:
        return None
