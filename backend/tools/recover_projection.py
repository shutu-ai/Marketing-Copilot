"""Safely recover a VNext projection without touching the SMS send path.

The service must be stopped. The current checkpointed DuckDB base is copied
without its WAL, immutable progress journals are replayed into that copy, and
the projection directory is promoted only after checkpoint and read-only
validation succeed. The old directory remains as a rollback/forensics copy.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


def _service_is_active(unit: str) -> bool:
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "--quiet", unit],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return False
    return result.returncode == 0


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def _validate_projection(db_path: Path) -> dict:
    import duckdb

    if Path(f"{db_path}.wal").exists():
        raise RuntimeError(f"recovery output still has a WAL: {db_path}.wal")
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        counts = {
            table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("vnext_progress_events", "vnext_projection", "vnext_task_index")
        }
    finally:
        connection.close()
    return counts


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recover and atomically promote a VNext projection.")
    parser.add_argument("--root", type=Path, default=Path(os.environ.get("MC_VNEXT_RUN_ROOT", "data/vnext")))
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--service-unit", default=os.environ.get("MC_SERVICE_UNIT", "marketing-copilot.service"))
    parser.add_argument("--workers", type=int, default=int(os.environ.get("MC_VNEXT_WORKERS", "4")))
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--checkpoint-every-events", type=int, default=100000)
    parser.add_argument("--progress-seconds", type=float, default=10.0)
    parser.add_argument("--no-promote", action="store_true", help="validate the rebuilt directory without replacing production")
    parser.add_argument(
        "--promote-existing",
        action="store_true",
        help="promote an already validated output directory without rebuilding it",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    source_root = args.root.expanduser().resolve()
    source_projection = source_root / "projection"
    if (source_projection / "vnext-projection.sqlite3").exists():
        print(json.dumps({"ok": False, "error": "sqlite_projection_requires_sqlite_recovery"}))
        return 2
    source_db = source_projection / "vnext-projection.duckdb"
    if _service_is_active(str(args.service_unit)):
        print(json.dumps({"ok": False, "error": "service_active", "service_unit": args.service_unit}))
        return 2
    if not source_db.is_file():
        print(json.dumps({"ok": False, "error": "projection_database_missing", "path": str(source_db)}))
        return 2

    stamp = time.strftime("%Y%m%d%H%M%S")
    output_root = (args.output_root or source_root.parent / f".vnext-recovery-{stamp}").expanduser().resolve()
    backup_projection = source_root.parent / f".vnext-projection-backup-{stamp}"
    seed_projection = source_root.parent / f".vnext-seed-{stamp}.duckdb"
    if args.promote_existing and args.output_root is None:
        print(json.dumps({"ok": False, "error": "promote_existing_requires_output_root"}))
        return 2
    if args.promote_existing:
        if backup_projection.exists() or seed_projection.exists():
            print(json.dumps({"ok": False, "error": "recovery_path_exists", "output_root": str(output_root)}))
            return 2
        existing_state_path = output_root / "recovery-state.json"
        try:
            existing_state = json.loads(existing_state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            print(json.dumps({"ok": False, "error": "validated_recovery_state_missing", "detail": str(exc)}))
            return 2
        if existing_state.get("ok") is not True or existing_state.get("phase") != "validated":
            print(json.dumps({"ok": False, "error": "recovery_output_not_validated", "path": str(output_root)}))
            return 2
    elif output_root.exists() or backup_projection.exists() or seed_projection.exists():
        print(json.dumps({"ok": False, "error": "recovery_path_exists", "output_root": str(output_root)}))
        return 2

    state_path = output_root / "recovery-state.json"
    if not args.promote_existing:
        shutil.copy2(source_db, seed_projection)

        command = [
            sys.executable,
            str(Path(__file__).with_name("bulk_rebuild_projection.py")),
            "--root", str(source_root),
            "--output-root", str(output_root),
            "--seed-projection", str(seed_projection),
            "--service-unit", str(args.service_unit),
            "--workers", str(max(1, int(args.workers))),
            "--batch-size", str(max(1, int(args.batch_size))),
            "--checkpoint-every-events", str(max(0, int(args.checkpoint_every_events))),
            "--progress-seconds", str(max(0.5, float(args.progress_seconds))),
            "--threads", "1",
        ]
        memory_limit = str(os.environ.get("MC_VNEXT_PROJECTION_MAINTENANCE_MEMORY_LIMIT", "16GB"))
        command.extend(["--memory-limit", memory_limit])
        result = subprocess.run(command, check=False)
        if result.returncode != 0:
            _write_json(state_path, {"ok": False, "phase": "bulk_rebuild", "returncode": result.returncode})
            return result.returncode or 1
        seed_projection.unlink(missing_ok=True)

    output_db = output_root / "projection" / source_db.name
    counts = _validate_projection(output_db)
    validation = {"ok": True, "phase": "validated", "counts": counts}
    _write_json(state_path, validation)
    if args.no_promote:
        print(json.dumps({**validation, "promoted": False}, ensure_ascii=False))
        return 0
    if _service_is_active(str(args.service_unit)):
        _write_json(state_path, {**validation, "ok": False, "error": "service_restarted_before_promote"})
        return 2

    output_projection = output_root / "projection"
    os.rename(source_projection, backup_projection)
    try:
        os.rename(output_projection, source_projection)
    except BaseException:
        os.rename(backup_projection, source_projection)
        raise
    result = {
        **validation,
        "phase": "promoted",
        "promoted": True,
        "backup_projection": str(backup_projection),
        "source_root": str(source_root),
        "output_root": str(output_root),
    }
    _write_json(state_path, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
