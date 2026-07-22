"""Self-contained multi-file batch campaign E2E test with local SMSC."""

import argparse
import json
import logging
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta


def wait_for_port(host: str, port: int, timeout: float = 10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"SMSC simulator did not listen on {host}:{port}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Batch campaign end-to-end test")
    parser.add_argument("--port", type=int, default=2777)
    args = parser.parse_args()

    backend_dir = Path(__file__).resolve().parents[1]
    project_dir = backend_dir.parent
    old_dir = project_dir / "old_script"
    source_events = old_dir / "input_event.csv"
    source_whitelist = old_dir / "whitelist.csv"
    if not source_events.is_file() or not source_whitelist.is_file():
        raise SystemExit("old_script/input_event.csv or whitelist.csv not found")

    with tempfile.TemporaryDirectory(prefix="marketing-copilot-batch-e2e-") as temp_name:
        temp_dir = Path(temp_name)
        os.environ["DUCKDB_PATH"] = str(temp_dir / "e2e.duckdb")

        import pandas as pd
        from database import init_db, get_connection, reconnect
        from services import activity_scheduler, batch_campaign_service, settings_service, whitelist_service
        from services.task_worker import start_worker, stop_worker

        init_db()
        conn = get_connection()
        now = datetime.now()

        legacy_whitelist = pd.read_csv(source_whitelist, sep="|", dtype=str)
        generated_whitelist = legacy_whitelist.head(5).copy()
        global_whitelist_path = temp_dir / "global_whitelist.csv"
        generated_whitelist.to_csv(global_whitelist_path, sep="|", index=False)
        conn.execute("INSERT INTO upload_history (filename,status) VALUES (?,?)", ("generated_whitelist.csv", "pending"))
        upload_id = conn.lastrowid
        conn.commit()
        import_result = whitelist_service.import_file(upload_id, str(global_whitelist_path), "append")
        if import_result["success"] != 5:
            conn.close()
            reconnect(":memory:")
            raise AssertionError(import_result)
        check_conn = get_connection()
        imported_users = [dict(row) for row in check_conn.execute(
            "SELECT msisdn,language,status FROM whitelist_users ORDER BY id"
        ).fetchall()]
        check_conn.close()
        active_numbers = {
            __import__("services.sms_task_service", fromlist=["normalize_msisdn"]).normalize_msisdn(row["msisdn"])
            for row in imported_users if row["status"] == "active"
        }
        expected_numbers = {f"251{value}" for value in generated_whitelist["msisdn"].tolist()}
        if active_numbers != expected_numbers:
            conn.close()
            reconnect(":memory:")
            raise AssertionError({
                "message": "imported whitelist does not match expected normalized numbers",
                "imported_users": imported_users,
                "active_numbers": sorted(active_numbers),
                "expected_numbers": sorted(expected_numbers),
            })

        legacy_events = pd.read_csv(source_events, sep="|", dtype=str)
        whitelist_numbers = generated_whitelist["msisdn"].tolist()
        non_whitelist_a = legacy_events.iloc[7]["msisdn"]
        non_whitelist_b = legacy_events.iloc[8]["msisdn"]
        file_specs = [
            ("Batch E2E A", [whitelist_numbers[0], whitelist_numbers[1], whitelist_numbers[2],
                              whitelist_numbers[0], non_whitelist_a]),
            ("Batch E2E B", [whitelist_numbers[2], whitelist_numbers[3], whitelist_numbers[4],
                              whitelist_numbers[3], non_whitelist_b]),
        ]

        campaign_ids = []
        generated_files = []
        start_time = (now - timedelta(days=1)).isoformat(timespec="seconds")
        end_time = (now + timedelta(days=1)).isoformat(timespec="seconds")

        for campaign_index, (name, numbers) in enumerate(file_specs, start=1):
            campaign_dir = temp_dir / f"campaign_{campaign_index}"
            conn.execute(
                """INSERT INTO batch_campaigns
                   (name,start_time,end_time,send_start_time,send_end_time,send_days,
                    batch_size,status,campaign_dir)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (name, start_time, end_time, "00:00", "23:59", json.dumps([now.weekday()]),
                 2, "in_progress", str(campaign_dir)),
            )
            campaign_id = conn.lastrowid
            campaign_ids.append(campaign_id)
            conn.execute(
                "INSERT INTO campaigns (name,marketing_type,marketing_campaign_id,enabled) VALUES (?,?,?,1)",
                (f"{name} Content", "batch", campaign_id),
            )
            content_campaign_id = conn.lastrowid
            for language in sorted(set(generated_whitelist["language"].fillna("English"))):
                conn.execute(
                    """INSERT INTO sms_templates (campaign_id,language,content,source_addr,enabled)
                       VALUES (?,?,?,?,1)""",
                    (content_campaign_id, language,
                     f"{name}: event={{event_filter_id}}; cell={{cell_id}}; user={{msisdn}}",
                     "telebirr"),
                )
            conn.commit()

            generated = legacy_events.iloc[:5].copy()
            generated["msisdn"] = numbers
            input_path = temp_dir / f"generated_batch_{campaign_index}.csv"
            generated.to_csv(input_path, sep="|", index=False)
            generated_files.append(str(input_path))
            uploaded = batch_campaign_service.upload_customer_list(
                campaign_id, str(input_path), input_path.name
            )
            if not uploaded or uploaded["row_count"] != 5:
                conn.close()
                reconnect(":memory:")
                raise AssertionError(uploaded)

        conn.execute(
            """INSERT INTO smpp_configs
               (name,host,port,system_id,password,source_addr,bind_type,enabled)
               VALUES (?,?,?,?,?,?,?,1)""",
            ("Local SMSC", "127.0.0.1", args.port, "test", "test", "telebirr", "transceiver"),
        )
        conn.commit()
        settings_service.set_setting("system_mode", "production")

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
            start_worker()

            processing_results = []
            for _ in range(20):
                pending = conn.execute(
                    "SELECT COUNT(*) cnt FROM batch_campaign_batches WHERE status='pending'"
                ).fetchone()["cnt"]
                if pending == 0:
                    break
                result = activity_scheduler._process_batches()
                if result:
                    processing_results.append(result)
                time.sleep(0.1)
            else:
                raise AssertionError("batch scheduler did not consume pending batches")

            tasks = conn.execute("SELECT * FROM sms_tasks ORDER BY id").fetchall()
            assert len(tasks) == 6, {
                "tasks": [dict(row) for row in tasks],
                "processing_results": processing_results,
                "batches": [dict(row) for row in conn.execute(
                    "SELECT * FROM batch_campaign_batches ORDER BY id"
                ).fetchall()],
            }
            for task in tasks:
                assert task["batch_id"], dict(task)
                assert "{event_filter_id}" not in task["content"], task["content"]
                assert "{cell_id}" not in task["content"], task["content"]

            deadline = time.time() + 30
            state_map = {}
            while time.time() < deadline:
                states = conn.execute("SELECT status, COUNT(*) cnt FROM sms_tasks GROUP BY status").fetchall()
                state_map = {row["status"]: row["cnt"] for row in states}
                if state_map.get("delivered") == 6:
                    break
                time.sleep(0.2)
            else:
                diagnostic_tasks = [dict(row) for row in conn.execute(
                    """SELECT id,batch_id,status,attempts,smsc_message_ids,error_msg,updated_at
                       FROM sms_tasks ORDER BY id"""
                ).fetchall()]
                diagnostic_receipts = [dict(row) for row in conn.execute(
                    "SELECT * FROM delivery_receipts ORDER BY id"
                ).fetchall()]
                raise AssertionError({
                    "message": "tasks did not reach delivered",
                    "states": state_map,
                    "tasks": diagnostic_tasks,
                    "receipts": diagnostic_receipts,
                })

            receipt_count = conn.execute("SELECT COUNT(*) cnt FROM delivery_receipts").fetchone()["cnt"]
            assert receipt_count == 6, receipt_count

            campaign_results = []
            for campaign_id in campaign_ids:
                batch_rows = conn.execute(
                    "SELECT status, COUNT(*) cnt FROM batch_campaign_batches WHERE campaign_id=? GROUP BY status",
                    (campaign_id,),
                ).fetchall()
                batch_states = {row["status"]: row["cnt"] for row in batch_rows}
                assert batch_states == {"sent": 2, "skipped": 1}, batch_states
                stats = conn.execute(
                    "SELECT sent_count FROM batch_campaign_daily_stats WHERE campaign_id=?",
                    (campaign_id,),
                ).fetchone()
                assert stats and stats["sent_count"] == 3, dict(stats) if stats else None
                detail = batch_campaign_service.get_detail(campaign_id)
                assert detail["reached_users"] == 3, detail
                campaign_results.append({
                    "campaign_id": campaign_id,
                    "batch_states": batch_states,
                    "delivered_users": detail["reached_users"],
                    "daily_sent_count": stats["sent_count"],
                })

            final_tasks = [dict(row) for row in conn.execute(
                """SELECT id,campaign_id,batch_id,msisdn,language,content,status,smsc_message_ids
                   FROM sms_tasks ORDER BY id"""
            ).fetchall()]
            print(json.dumps({
                "result": "PASS",
                "generated_input_files": 2,
                "rows_per_file": 5,
                "campaigns": campaign_results,
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
