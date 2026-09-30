import shutil
import json
from database import get_connection
import os
import stat
import pandas as pd
from datetime import datetime
from typing import Optional
from services import settings_service


def _parse_targets(val) -> list:
    if not val:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return [val] if val.strip() else []
    return []


def _fmt_targets(targets: list) -> str:
    return json.dumps(targets, ensure_ascii=False)


def _enrich_config(cfg: dict) -> dict:
    raw = cfg.get("target_directories") or cfg.get("target_directory", "")
    cfg["target_directories"] = _parse_targets(raw)
    cfg.pop("target_directory", None)
    return cfg


def get_all_configs() -> list:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM monitor_configs ORDER BY created_at DESC").fetchall()
    conn.close()
    return [_enrich_config(dict(r)) for r in rows]


def add_config(directory_path: str, target_directories: list = None) -> dict:
    abs_path = os.path.abspath(directory_path)
    targets = target_directories or []
    conn = get_connection()
    existing = conn.execute(
        "SELECT * FROM monitor_configs WHERE directory_path=?", (abs_path,)
    ).fetchone()
    if existing:
        conn.close()
        return _enrich_config(dict(existing))

    cursor = conn.execute(
        "INSERT INTO monitor_configs (directory_path, target_directories) VALUES (?,?)",
        (abs_path, _fmt_targets(targets))
    )
    conn.commit()
    row = conn.execute("SELECT * FROM monitor_configs WHERE id=?", (cursor.lastrowid,)).fetchone()
    conn.close()
    return _enrich_config(dict(row))


def update_config(config_id: int, data: dict) -> Optional[dict]:
    allowed = {"directory_path", "target_directories", "enabled"}
    fields = {}
    for k, v in data.items():
        if k in allowed and v is not None:
            if k == "target_directories":
                fields["target_directories"] = _fmt_targets(v if isinstance(v, list) else [v])
            else:
                fields[k] = v

    if not fields:
        conn = get_connection()
        row = conn.execute("SELECT * FROM monitor_configs WHERE id=?", (config_id,)).fetchone()
        conn.close()
        return _enrich_config(dict(row)) if row else None

    if "directory_path" in fields:
        fields["directory_path"] = os.path.abspath(fields["directory_path"])

    conn = get_connection()
    fields["updated_at"] = "datetime('now','localtime')"
    set_clause = ", ".join(
        f"{k}=?" if k != "updated_at" else f"{k}=datetime('now','localtime')"
        for k in fields
    )
    values = [v for k, v in fields.items() if k != "updated_at"]
    conn.execute(f"UPDATE monitor_configs SET {set_clause} WHERE id=?", (*values, config_id))
    conn.commit()
    row = conn.execute("SELECT * FROM monitor_configs WHERE id=?", (config_id,)).fetchone()
    conn.close()
    return _enrich_config(dict(row)) if row else None


def toggle_config(config_id: int, enabled: int) -> Optional[dict]:
    conn = get_connection()
    conn.execute(
        "UPDATE monitor_configs SET enabled=?, updated_at=datetime('now','localtime') WHERE id=?",
        (enabled, config_id)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM monitor_configs WHERE id=?", (config_id,)).fetchone()
    conn.close()
    return _enrich_config(dict(row)) if row else None


def delete_config(config_id: int) -> bool:
    conn = get_connection()
    conn.execute("DELETE FROM monitor_files WHERE monitor_config_id=?", (config_id,))
    cursor = conn.execute("DELETE FROM monitor_configs WHERE id=?", (config_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def _move_file(source_path: str, target_dir: str, copy_from: str = None) -> str:
    os.makedirs(target_dir, exist_ok=True)
    fname = os.path.basename(source_path)
    dest = os.path.join(target_dir, fname)
    if copy_from:
        shutil.copy2(copy_from, dest)
    else:
        shutil.move(source_path, dest)
    return dest


def _distribute_to_targets(source_path: str, target_dirs: list) -> tuple:
    """Distribute file to all targets. First target uses move, rest copy from first.
    Returns (destinations, moved_to_first) where moved_to_first indicates file was moved to first target."""
    destinations = []
    first_dest = None
    fname = os.path.basename(source_path)
    for tdir in target_dirs:
        try:
            if first_dest is None:
                dest = _move_file(source_path, tdir)
                first_dest = dest
            else:
                dest = _move_file(source_path, tdir, copy_from=first_dest)
            destinations.append(dest)
        except Exception:
            pass
    return destinations, first_dest is not None


def _redistribute_pending(conn, directory_path: str, target_dirs: list, config_id: int) -> list:
    """Attempt to distribute existing pending files whose source still exists."""
    updated = []
    rows = conn.execute(
        "SELECT * FROM monitor_files WHERE directory_path=? AND monitor_config_id=? AND status IN ('pending','processing')",
        (directory_path, config_id)
    ).fetchall()
    for r in rows:
        f = dict(r)
        src = os.path.join(f["directory_path"], f["filename"])
        if not os.path.isfile(src):
            continue
        moved_dirs, _ = _distribute_to_targets(src, target_dirs)
        if moved_dirs:
            conn.execute(
                "UPDATE monitor_files SET status='distributed', updated_at=datetime('now','localtime') WHERE id=?",
                (f["id"],)
            )
            updated.append(f["id"])
    return updated


def scan_directory(config_id: int, directory_path: str) -> dict:
    """Scan and auto-distribute new files to all target directories."""
    conn = get_connection()
    new_files = []
    moved_count = 0

    if not os.path.isdir(directory_path):
        conn.close()
        raise FileNotFoundError(f"Directory does not exist: {directory_path}")

    config_row = conn.execute(
        "SELECT * FROM monitor_configs WHERE id=?", (config_id,)
    ).fetchone()
    target_dirs = _parse_targets(config_row["target_directories"] if config_row else "")

    # Retry distribution for existing pending files
    if target_dirs:
        redone = _redistribute_pending(conn, directory_path, target_dirs, config_id)
        if redone:
            moved_count += len(redone)
        conn.commit()

    for fname in os.listdir(directory_path):
        if not fname.lower().endswith(('.csv', '.xlsx', '.xls')):
            continue

        fpath = os.path.join(directory_path, fname)
        if not os.path.isfile(fpath):
            continue

        existing = conn.execute(
            "SELECT id, status FROM monitor_files WHERE filename=? AND directory_path=?",
            (fname, directory_path)
        ).fetchone()
        if existing:
            continue

        fsize = os.path.getsize(fpath)
        row_count = _count_rows(fpath)
        moved_dirs, _ = _distribute_to_targets(fpath, target_dirs)
        status = "distributed" if moved_dirs else "pending"

        try:
            cursor = conn.execute(
                """INSERT INTO monitor_files (filename, directory_path, file_size, row_count, status, monitor_config_id)
                   VALUES (?,?,?,?,?,?)""",
                (fname, directory_path, fsize, row_count, status, config_id)
            )
            row_id = cursor.lastrowid
        except Exception:
            conn.conn.rollback()
            continue

        new_files.append({
            "id": row_id,
            "filename": fname,
            "directory_path": directory_path,
            "file_size": fsize,
            "row_count": row_count,
            "status": status,
            "moved_to": moved_dirs,
        })
        if moved_dirs:
            moved_count += 1

    conn.commit()
    conn.close()
    return {"new_files": new_files, "count": len(new_files), "moved_count": moved_count}


def distribute_file(file_id: int) -> dict:
    conn = get_connection()
    row = conn.execute("SELECT * FROM monitor_files WHERE id=?", (file_id,)).fetchone()
    if not row:
        conn.close()
        return {"success": False, "message": "File not found"}
    file_info = dict(row)

    config_row = conn.execute(
        "SELECT * FROM monitor_configs WHERE id=?", (file_info.get("monitor_config_id"),)
    ).fetchone()
    conn.close()
    if not config_row:
        return {"success": False, "message": "Monitor config not found"}
    config = dict(config_row)

    target_dirs = _parse_targets(config.get("target_directories", ""))
    if not target_dirs:
        return {"success": False, "message": "No target directories configured"}

    source_path = os.path.join(file_info["directory_path"], file_info["filename"])
    if not os.path.isfile(source_path):
        return {"success": False, "message": f"Source file does not exist: {source_path}"}

    destinations, moved = _distribute_to_targets(source_path, target_dirs)

    return {
        "success": True,
        "message": f"Distributed to {len(destinations)} target(s)",
        "destinations": destinations,
    }


def get_files(status: Optional[str] = None, directory_path: Optional[str] = None) -> list:
    conn = get_connection()
    conditions = []
    params = []
    if status:
        conditions.append("status=?")
        params.append(status)
    if directory_path:
        conditions.append("directory_path=?")
        params.append(directory_path)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    rows = conn.execute(
        f"SELECT * FROM monitor_files {where} ORDER BY created_at DESC", tuple(params)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def sync_source_files() -> dict:
    """Check source files against monitor_files; mark as distributed if missing."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM monitor_files WHERE status='pending' OR status='processing'"
    ).fetchall()
    updated = 0
    for r in rows:
        f = dict(r)
        src_path = os.path.join(f["directory_path"], f["filename"])
        if not os.path.isfile(src_path):
            conn.execute(
                "UPDATE monitor_files SET status='distributed', updated_at=datetime('now','localtime') WHERE id=?",
                (f["id"],)
            )
            updated += 1
    conn.commit()
    conn.close()
    return {"updated": updated}


def update_file_status(file_id: int, status: str) -> bool:
    conn = get_connection()
    conn.execute(
        "UPDATE monitor_files SET status=?, updated_at=datetime('now','localtime') WHERE id=?",
        (status, file_id)
    )
    conn.commit()
    conn.close()
    return True


def process_file(file_id: int, file_path: str, campaign_name: str = "") -> dict:
    """
    Read CSV/Excel rows and publish each to RabbitMQ with routing_key = campaign.{campaign_name}
    The filename (minus ext) is used as campaign_name if not provided.
    """
    if not campaign_name:
        campaign_name = os.path.splitext(os.path.basename(file_path))[0]

    try:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.csv':
            df = pd.read_csv(file_path, dtype=str)
        elif ext in ('.xlsx', '.xls'):
            df = pd.read_excel(file_path, dtype=str)
        else:
            return {"success": False, "message": "Unsupported file type"}

        df = df.fillna("")
        published = 0
        routing_key = f"campaign.{campaign_name}"

        for _, row in df.iterrows():
            message = {k: str(v) for k, v in row.items()}
            if message.get("msisdn", "").strip():
                from services.rabbitmq_service import publish
                ok = publish(routing_key, message)
                if ok:
                    published += 1

        update_file_status(file_id, "completed")

        return {
            "success": True,
            "total_rows": len(df),
            "published": published,
            "routing_key": routing_key,
            "campaign": campaign_name,
        }
    except Exception as e:
        update_file_status(file_id, "failed")
        return {"success": False, "message": str(e)}


def _count_rows(file_path: str) -> int:
    try:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.csv':
            return len(pd.read_csv(file_path))
        elif ext in ('.xlsx', '.xls'):
            return len(pd.read_excel(file_path))
    except Exception:
        pass
    return 0


def get_realtime_monitor(
    campaign_id: Optional[int] = None,
    source_page: int = 1,
    source_page_size: int = 50,
    processed_page: int = 1,
    processed_page_size: int = 50,
) -> dict:
    source_files = []
    conn = get_connection()
    all_campaign_rows = conn.execute(
        "SELECT id,name,status,watch_source,watch_dir,watch_filename_prefix,sftp_host,sftp_port,sftp_username FROM realtime_campaigns ORDER BY created_at DESC"
    ).fetchall()
    if campaign_id:
        campaign_rows = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE id=? AND COALESCE(watch_dir,'')<>'' ORDER BY created_at DESC",
            (campaign_id,),
        ).fetchall()
    else:
        campaign_rows = conn.execute(
            "SELECT * FROM realtime_campaigns WHERE COALESCE(watch_dir,'')<>'' ORDER BY created_at DESC"
        ).fetchall()
    active_campaigns = conn.execute("SELECT COUNT(*) AS count FROM realtime_campaigns WHERE status='in_progress'").fetchone()
    processed_where = "campaign_type='realtime'"
    processed_params = []
    if campaign_id:
        processed_where += " AND campaign_id=?"
        processed_params.append(campaign_id)
    processed_total = conn.execute(
        f"SELECT COUNT(*) cnt FROM processed_files WHERE {processed_where}",
        tuple(processed_params),
    ).fetchone()["cnt"]
    processed_offset = max(0, processed_page - 1) * processed_page_size
    processed_rows = conn.execute(
        f"""SELECT * FROM processed_files
            WHERE {processed_where}
            ORDER BY detected_at DESC LIMIT ? OFFSET ?""",
        tuple(processed_params + [processed_page_size, processed_offset]),
    ).fetchall()
    ingestion_where = "campaign_type='realtime'"
    ingestion_params = []
    if campaign_id:
        ingestion_where += " AND campaign_id=?"
        ingestion_params.append(campaign_id)
    ingestion_rows = conn.execute(
        f"""SELECT * FROM ingestion_files WHERE {ingestion_where}
            ORDER BY updated_at DESC LIMIT 100""", tuple(ingestion_params),
    ).fetchall()
    conn.close()
    ingestion_by_key = {(int(item["campaign_id"]), item["filename"]): item["id"] for item in (dict(row) for row in ingestion_rows)}
    processed_items = []
    for row in processed_rows:
        item = dict(row)
        item["ingestion_file_id"] = ingestion_by_key.get((int(item["campaign_id"]), item["filename"]))
        processed_items.append(item)

    selected_campaign = dict(campaign_rows[0]) if campaign_id and campaign_rows else None
    watch_dirs = {}
    for row in campaign_rows:
        campaign = dict(row)
        source = campaign.get("watch_source", "local")
        watch_dir = (f"sftp://{campaign.get('sftp_host')}:{campaign.get('sftp_port')}{campaign.get('watch_dir')}"
                     if source == "sftp" else os.path.abspath(campaign.get("watch_dir", "")))
        item = watch_dirs.setdefault(watch_dir, {
            "directory_path": watch_dir,
            "exists": os.path.isdir(campaign.get("watch_dir", "")) if source == "local" else None,
            "source": source,
            "error": "",
            "config": campaign,
            "campaigns": [],
        })
        item["campaigns"].append({
            "id": campaign["id"],
            "name": campaign["name"],
            "status": campaign["status"],
            "watch_filename_prefix": campaign.get("watch_filename_prefix", ""),
        })

    for watch_dir, meta in watch_dirs.items():
        if meta["source"] == "sftp":
            client = sftp = None
            try:
                from services import sftp_watch_service
                client, sftp, entries = sftp_watch_service.list_directory(meta["config"])
                meta["exists"] = True
                if selected_campaign:
                    from services import realtime_campaign_service
                    for entry in entries:
                        name = entry.filename
                        if not stat.S_ISREG(entry.st_mode) or os.path.splitext(name)[1].lower() != ".csv":
                            continue
                        match_prefix = bool(selected_campaign.get("watch_filename_prefix") and name.startswith(selected_campaign["watch_filename_prefix"]))
                        match_schedule = realtime_campaign_service._watch_file_matches_campaign(name, selected_campaign)
                        processed = realtime_campaign_service._processed_file_record(selected_campaign["id"], name)
                        status = "processed" if processed else ("pending" if match_prefix and match_schedule else "ignored")
                        source_files.append({
                            "filename": name, "directory_path": watch_dir,
                            "file_size": entry.st_size or 0, "row_count": 0,
                            "match_prefix": match_prefix, "match_schedule": match_schedule,
                            "status": status, "status_label": status.title(), "skip_reason": "",
                            "created_at": "", "updated_at": "",
                        })
            except Exception as exc:
                meta["exists"] = False
                meta["error"] = str(exc)
            finally:
                if sftp:
                    sftp.close()
                if client:
                    client.close()
            meta.pop("config", None)
            continue
        if not meta["exists"]:
            continue
        for name in sorted(os.listdir(watch_dir)):
            path = os.path.join(watch_dir, name)
            if not os.path.isfile(path) or os.path.splitext(name)[1].lower() != ".csv":
                continue
            match_prefix = None
            match_schedule = None
            if selected_campaign:
                from services import realtime_campaign_service
                prefix = selected_campaign.get("watch_filename_prefix", "")
                match_prefix = bool(prefix and name.startswith(prefix))
                match_schedule = realtime_campaign_service._watch_file_matches_campaign(name, selected_campaign)
                processed_record = realtime_campaign_service._processed_file_record(selected_campaign["id"], name)
                if processed_record:
                    status = "processed"
                    status_label = "已处理过"
                    skip_reason = f"同名文件已处理: {processed_record.get('filename')}"
                elif not match_prefix:
                    status = "ignored"
                    status_label = "前缀不匹配"
                    skip_reason = "文件名不符合当前活动前缀"
                elif not match_schedule:
                    status = "ignored"
                    status_label = "发送规则不匹配"
                    skip_reason = "文件名时间不在当前活动发送日期或时间段内"
                elif datetime.now().timestamp() - os.path.getmtime(path) < 2:
                    status = "pending"
                    status_label = "写入中"
                    skip_reason = "文件仍在写入稳定等待期内"
                else:
                    status = "pending"
                    status_label = "待处理"
                    skip_reason = ""
            else:
                status = "watching"
                status_label = "监听中"
                skip_reason = ""
            source_files.append({
                "filename": name,
                "directory_path": watch_dir,
                "file_size": os.path.getsize(path),
                "row_count": _count_rows(path),
                "match_prefix": match_prefix,
                "match_schedule": match_schedule,
                "status": status,
                "status_label": status_label,
                "skip_reason": skip_reason,
                "created_at": "",
                "updated_at": "",
            })

    source_files.sort(key=lambda item: item["filename"])
    source_files_total = len(source_files)
    source_offset = max(0, source_page - 1) * source_page_size
    source_files = source_files[source_offset:source_offset + source_page_size]

    return {
        "campaign_id": campaign_id,
        "watch_dir": "",
        "exists": all(item["exists"] for item in watch_dirs.values()) if watch_dirs else False,
        "watch_dirs": list(watch_dirs.values()),
        "campaigns": [dict(row) for row in all_campaign_rows],
        "active_campaigns": active_campaigns["count"] if active_campaigns else 0,
        "source_files": source_files,
        "source_files_total": source_files_total,
        "source_files_page": source_page,
        "source_files_page_size": source_page_size,
        "processed_files": processed_items,
        "processed_files_total": processed_total,
        "processed_files_page": processed_page,
        "processed_files_page_size": processed_page_size,
        "ingestion_files": [dict(row) for row in ingestion_rows],
    }


def scan_realtime_watch_dir(
    campaign_id: Optional[int] = None,
    source_page: int = 1,
    source_page_size: int = 50,
    processed_page: int = 1,
    processed_page_size: int = 50,
) -> dict:
    from services import realtime_campaign_service
    sync_result = realtime_campaign_service.sync_watch_dirs_to_inboxes(campaign_id=campaign_id)
    process_result = realtime_campaign_service.process_realtime_inboxes(campaign_id=campaign_id)
    return {
        "sync": sync_result,
        "processing": process_result,
        "monitor": get_realtime_monitor(
            campaign_id,
            source_page,
            source_page_size,
            processed_page,
            processed_page_size,
        ),
    }


def retry_realtime_file(file_id: int) -> Optional[dict]:
    from services import ingestion_job_service
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM ingestion_files WHERE id=? AND campaign_type='realtime'", (file_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    job = dict(row)
    if job["status"] not in ("failed", "quarantined", "retrying"):
        return {"success": False, "message": "only failed or quarantined files can be retried"}
    campaign = get_connection()
    try:
        campaign_row = campaign.execute("SELECT campaign_dir FROM realtime_campaigns WHERE id=?", (job["campaign_id"],)).fetchone()
    finally:
        campaign.close()
    campaign_dir = campaign_row["campaign_dir"] if campaign_row else ""
    inbox_path = os.path.join(campaign_dir, "inbox", job["filename"])
    if not os.path.isfile(inbox_path):
        quarantine_dir = os.path.join(campaign_dir, "quarantine")
        quarantine_path = os.path.join(quarantine_dir, job["filename"])
        if not os.path.isfile(quarantine_path) and os.path.isdir(quarantine_dir):
            stem, ext = os.path.splitext(job["filename"])
            matches = sorted(
                name for name in os.listdir(quarantine_dir)
                if name.startswith(stem + "_") and name.endswith(ext)
            )
            if matches:
                quarantine_path = os.path.join(quarantine_dir, matches[-1])
        if os.path.isfile(quarantine_path):
            os.makedirs(os.path.dirname(inbox_path), exist_ok=True)
            shutil.move(quarantine_path, inbox_path)
    if not os.path.isfile(inbox_path):
        return {"success": False, "message": "source file is no longer available"}
    result = ingestion_job_service.requeue_file(file_id)
    if not result:
        return None
    conn = get_connection()
    try:
        conn.execute("UPDATE processed_files SET status='queued',processing_phase='queued',next_retry_at=NULL WHERE campaign_type='realtime' AND campaign_id=? AND filename=?",
                     (job["campaign_id"], job["filename"]))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "file": result}


def retry_realtime_files(file_ids: list[int]) -> dict:
    results = []
    for file_id in list(dict.fromkeys(int(value) for value in file_ids)):
        results.append({"file_id": file_id, **(retry_realtime_file(file_id) or {
            "success": False, "message": "realtime ingestion file not found"
        })})
    return {
        "success": all(item.get("success") for item in results) if results else True,
        "requested": len(results),
        "requeued": sum(1 for item in results if item.get("success")),
        "items": results,
    }


def archive_realtime_file(file_id: int) -> Optional[dict]:
    """Archive a terminal realtime file without deleting its audit metadata."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM ingestion_files WHERE id=? AND campaign_type='realtime'", (file_id,)
        ).fetchone()
        if not row:
            return None
        job = dict(row)
        if job["status"] not in ("failed", "quarantined"):
            return {"success": False, "message": "only failed or quarantined files can be archived"}
        campaign = conn.execute(
            "SELECT campaign_dir FROM realtime_campaigns WHERE id=?", (job["campaign_id"],)
        ).fetchone()
    finally:
        conn.close()
    campaign_dir = campaign["campaign_dir"] if campaign else ""
    candidates = [job.get("file_path", ""),
                  os.path.join(campaign_dir, "inbox", job["filename"]),
                  os.path.join(campaign_dir, "quarantine", job["filename"])]
    source = next((path for path in candidates if path and os.path.isfile(path)), "")
    target = source
    if source:
        archive_dir = os.path.join(campaign_dir, "archive")
        os.makedirs(archive_dir, exist_ok=True)
        target = os.path.join(archive_dir, os.path.basename(source))
        if os.path.exists(target):
            stem, ext = os.path.splitext(target)
            target = f"{stem}_{file_id}{ext}"
        shutil.move(source, target)
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE ingestion_files SET status='archived',processing_phase='archived',file_path=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (target, file_id),
        )
        conn.commit()
    finally:
        conn.close()
    processed = get_connection()
    try:
        processed.execute(
            "UPDATE processed_files SET status='archived',processing_phase='archived',file_path=?,completed_at=COALESCE(completed_at,CAST(CURRENT_TIMESTAMP AS VARCHAR)) WHERE campaign_type='realtime' AND campaign_id=? AND filename=?",
            (target, job["campaign_id"], job["filename"]),
        )
        processed.commit()
    finally:
        processed.close()
    return {"success": True, "file_id": file_id, "archive_path": target}


def archive_realtime_files(file_ids: list[int]) -> dict:
    results = []
    for file_id in list(dict.fromkeys(int(value) for value in file_ids)):
        results.append({"file_id": file_id, **(archive_realtime_file(file_id) or {
            "success": False, "message": "realtime ingestion file not found"
        })})
    return {
        "success": all(item.get("success") for item in results) if results else True,
        "requested": len(results),
        "archived": sum(1 for item in results if item.get("success")),
        "items": results,
    }
