"""Self-contained realtime campaign E2E test using the local SMSC simulator."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta


def wait_for_port(host: str, port: int, timeout: float = 10):
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"SMSC simulator did not listen on {host}:{port}")


def main():
    parser = argparse.ArgumentParser(description="Realtime campaign end-to-end test")
    parser.add_argument("--port", type=int, default=2776)
    args = parser.parse_args()

    backend_dir = Path(__file__).resolve().parents[1]
    project_dir = backend_dir.parent
    old_dir = project_dir / "old_script"
    source_events = old_dir / "input_event.csv"
    source_whitelist = old_dir / "whitelist.csv"
    if not source_events.is_file() or not source_whitelist.is_file():
        raise SystemExit("old_script/input_event.csv or whitelist.csv not found")

    with tempfile.TemporaryDirectory(prefix="marketing-copilot-realtime-e2e-") as temp_name:
        temp_dir = Path(temp_name)
        os.environ["DUCKDB_PATH"] = str(temp_dir / "e2e.duckdb")

        import pandas as pd
        from database import init_db, get_connection, reconnect
        from services import activity_scheduler, realtime_campaign_service, settings_service, whitelist_service
        from services.task_worker import start_worker, stop_worker

        init_db()
        conn = get_connection()
        now = datetime.now()
        campaign_dir = temp_dir / "realtime_campaign"
        inbox_dir = campaign_dir / "inbox"
        inbox_dir.mkdir(parents=True)

        # Generate a small global/activity whitelist based on the legacy sample.
        legacy_whitelist = pd.read_csv(source_whitelist, sep="|", dtype=str)
        generated_whitelist = legacy_whitelist.head(3).copy()
        global_whitelist_path = temp_dir / "global_whitelist.csv"
        generated_whitelist.to_csv(global_whitelist_path, sep="|", index=False)

        conn.execute("INSERT INTO upload_history (filename,status) VALUES (?,?)", ("generated_whitelist.csv", "pending"))
        upload_id = conn.lastrowid
        conn.commit()
        import_result = whitelist_service.import_file(upload_id, str(global_whitelist_path), "append")
        if import_result["success"] != 3:
            conn.close()
            reconnect(":memory:")
            raise AssertionError(import_result)

        start_time = (now - timedelta(days=1)).isoformat(timespec="seconds")
        end_time = (now + timedelta(days=1)).isoformat(timespec="seconds")
        conn.execute(
            """INSERT INTO realtime_campaigns
               (name,start_time,end_time,send_start_time,send_end_time,send_days,status,campaign_dir)
               VALUES (?,?,?,?,?,?,?,?)""",
            ("Realtime E2E", start_time, end_time, "00:00", "23:59",
             json.dumps([now.weekday()]), "in_progress", str(campaign_dir)),
        )
        campaign_id = conn.lastrowid
        conn.commit()

        activity_whitelist_upload = temp_dir / "activity_whitelist.csv"
        generated_whitelist.to_csv(activity_whitelist_upload, sep="|", index=False)
        uploaded = realtime_campaign_service.upload_customer_list(
            campaign_id, str(activity_whitelist_upload), "generated_whitelist.csv"
        )
        if not uploaded or uploaded["row_count"] != 3:
            conn.close()
            reconnect(":memory:")
            raise AssertionError(uploaded)

        conn.execute(
            "INSERT INTO campaigns (name,marketing_type,marketing_campaign_id,enabled) VALUES (?,?,?,1)",
            ("Realtime E2E Content", "realtime", campaign_id),
        )
        content_campaign_id = conn.lastrowid
        for language in sorted(set(generated_whitelist["language"].fillna("English"))):
            conn.execute(
                """INSERT INTO sms_templates (campaign_id,language,content,source_addr,enabled)
                   VALUES (?,?,?,?,1)""",
                (content_campaign_id, language,
                 "E2E event={event_filter_id}; cell={cell_id}; user={msisdn}", "telebirr"),
            )
        conn.execute(
            """INSERT INTO smpp_configs
               (name,host,port,system_id,password,source_addr,bind_type,enabled)
               VALUES (?,?,?,?,?,?,?,1)""",
            ("Local SMSC", "127.0.0.1", args.port, "test", "test", "telebirr", "transceiver"),
        )
        conn.commit()
        settings_service.set_setting("system_mode", "production")

        # Generate the realtime input using legacy columns and values.
        legacy_events = pd.read_csv(source_events, sep="|", dtype=str)
        test_rows = legacy_events.head(4).copy()
        whitelist_numbers = generated_whitelist["msisdn"].tolist()
        for index, msisdn in enumerate(whitelist_numbers):
            test_rows.loc[index, "msisdn"] = msisdn
        test_rows.loc[3, "msisdn"] = whitelist_numbers[0]  # duplicate
        non_whitelist = legacy_events.iloc[[4]].copy()
        generated_events = pd.concat([test_rows, non_whitelist], ignore_index=True)
        input_path = inbox_dir / "generated_realtime_event.csv"
        generated_events.to_csv(input_path, sep="|", index=False)

        simulator_python = getattr(sys, "_base_executable", sys.executable)
        simulator = subprocess.Popen(
            [simulator_python, "-u", "-m", "tools.smsc_simulator", "--port", str(args.port),
             "--system-id", "test", "--password", "test", "--receipt-delay", "0.2"],
            cwd=backend_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            encoding="utf-8", errors="replace",
        )
        simulator_lines = []
        output_thread = threading.Thread(
            target=lambda: simulator_lines.extend(iter(simulator.stdout.readline, "")), daemon=True
        )
        output_thread.start()
        try:
            wait_for_port("127.0.0.1", args.port)
            time.sleep(2.1)  # scheduler only reads stable files
            activity_scheduler._process_realtime_inboxes()

            file_row = conn.execute(
                "SELECT * FROM realtime_campaign_files WHERE campaign_id=?", (campaign_id,)
            ).fetchone()
            assert file_row, "realtime file was not processed"
            assert file_row["total_msisdn"] == 4, dict(file_row)
            assert file_row["matched_count"] == 3, dict(file_row)

            tasks = conn.execute("SELECT * FROM sms_tasks ORDER BY id").fetchall()
            assert len(tasks) == 3, [dict(row) for row in tasks]
            for task in tasks:
                assert "{event_filter_id}" not in task["content"], task["content"]
                assert "{cell_id}" not in task["content"], task["content"]

            start_worker()
            deadline = time.time() + 20
            while time.time() < deadline:
                states = conn.execute("SELECT status, COUNT(*) cnt FROM sms_tasks GROUP BY status").fetchall()
                state_map = {row["status"]: row["cnt"] for row in states}
                if state_map.get("delivered") == 3:
                    break
                time.sleep(0.2)
            else:
                raise AssertionError(f"tasks did not reach delivered: {state_map}")

            receipt_count = conn.execute("SELECT COUNT(*) cnt FROM delivery_receipts").fetchone()["cnt"]
            assert receipt_count == 3, receipt_count
            archived = campaign_dir / "received" / input_path.name
            assert archived.is_file(), f"received file not archived: {archived}"

            # Reintroducing identical content must move it out of inbox without
            # creating another processed-file row or another set of tasks.
            shutil.copy2(archived, input_path)
            old_mtime = time.time() - 3
            os.utime(input_path, (old_mtime, old_mtime))
            activity_scheduler._process_realtime_inboxes()
            assert not input_path.exists(), "duplicate file remained in watched inbox"
            duplicates = list((campaign_dir / "duplicates").glob("generated_realtime_event*.csv"))
            assert len(duplicates) == 1, duplicates
            assert conn.execute("SELECT COUNT(*) cnt FROM realtime_campaign_files").fetchone()["cnt"] == 1
            assert conn.execute("SELECT COUNT(*) cnt FROM sms_tasks").fetchone()["cnt"] == 3

            final_tasks = [dict(row) for row in conn.execute(
                "SELECT id,msisdn,language,content,status,smsc_message_ids FROM sms_tasks ORDER BY id"
            ).fetchall()]
            print(json.dumps({
                "result": "PASS",
                "generated_input_rows": len(generated_events),
                "unique_event_msisdn": file_row["total_msisdn"],
                "activity_whitelist_matched": file_row["matched_count"],
                "tasks_created": len(final_tasks),
                "delivery_receipts": receipt_count,
                "tasks": final_tasks,
            }, ensure_ascii=False, indent=2))
        finally:
            stop_worker()
            if simulator.poll() is None:
                simulator.terminate()
            try:
                simulator.wait(timeout=5)
            except subprocess.TimeoutExpired:
                simulator.kill()
                simulator.wait(timeout=5)
            output_thread.join(timeout=2)
            simulator_output = "".join(simulator_lines)
            print("\n--- LOCAL SMSC OUTPUT ---")
            print(simulator_output)
            conn.close()
            reconnect(":memory:")


if __name__ == "__main__":
    main()
