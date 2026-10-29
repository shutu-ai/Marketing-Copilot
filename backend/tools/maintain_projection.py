'''Offline VNext projection replay and checkpoint maintenance.

This command is intentionally separate from application startup. It must be
run while the Marketing Copilot service is stopped and performs the long
journal replay in an operator-controlled maintenance window. Normal writer
transactions only append durable WAL commits; this command performs the one
explicit checkpoint after replay has caught up.
'''

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from vnext.projection_writer import (
    ProjectionWriter,
    ProgressJournal,
    _load_offsets,
    _persist_offsets,
)
from vnext.task_index import rebuild_from_duckdb


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_vnext_root() -> Path:
    configured = str(os.environ.get('MC_VNEXT_ROOT') or '').strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return (_project_root() / 'data' / 'vnext').resolve()


def _service_is_active(unit: str) -> bool:
    if os.name != 'posix':
        return False
    try:
        result = subprocess.run(
            ['systemctl', 'is-active', '--quiet', unit],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return False
    return result.returncode == 0


def _journal_paths(progress_root: Path, worker_count: int) -> tuple[Path, ...]:
    return tuple(
        (progress_root / f'worker-{index}.jsonl').resolve()
        for index in range(max(1, int(worker_count)))
    )


def _storage(journals: tuple[ProgressJournal, ...], offsets: dict[str, int]) -> dict:
    total = committed = backlog = estimate = 0
    for journal in journals:
        snapshot = journal.storage_snapshot(offsets)
        total += int(snapshot['total_bytes'])
        committed += int(snapshot['committed_bytes'])
        backlog += int(snapshot['backlog_bytes'])
        estimate += int(snapshot['backlog_events_estimate'])
    return {
        'total_bytes': total,
        'committed_bytes': committed,
        'backlog_bytes': backlog,
        'backlog_events_estimate': estimate,
    }


def _read_quantum(
    journals: tuple[ProgressJournal, ...],
    offsets: dict[str, int],
    cursor: int,
    batch_size: int,
) -> tuple[list[dict], dict[str, int], int]:
    '''Read one fair, bounded replay quantum from all worker journals.'''

    count = len(journals)
    budget = max(1, int(batch_size))
    quantum = budget if count == 1 else max(64, min(256, math.ceil(budget / count)))
    events: list[dict] = []
    candidates: dict[str, int] = {}
    for offset in range(count):
        if budget <= 0:
            break
        index = (cursor + offset) % count
        journal = journals[index]
        remaining = min(quantum, budget)
        for segment_path in journal.segment_paths():
            if remaining <= 0 or not segment_path.exists():
                break
            key = str(segment_path)
            current = max(0, int(offsets.get(key, 0)))
            try:
                segment_size = int(segment_path.stat().st_size)
            except OSError:
                continue
            batch, new_offset = journal.read_segment_from(
                segment_path, current, max_events=remaining
            )
            if batch:
                events.extend(batch)
                candidates[key] = new_offset
                budget -= len(batch)
                remaining -= len(batch)
            if not batch and current < segment_size:
                # A partial/non-newline tail is not safe to skip. The next
                # invocation will retry the same byte boundary.
                break
    return events, candidates, (cursor + 1) % count


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Replay VNext progress journals and checkpoint the projection while the service is stopped.'
    )
    parser.add_argument('--root', type=Path, default=_default_vnext_root())
    parser.add_argument(
        '--output-root',
        type=Path,
        default=None,
        help=(
            'write the rebuilt projection to a separate VNext root while '
            'reading journals from --root; the target must be empty'
        ),
    )
    parser.add_argument(
        '--service-unit',
        default=os.environ.get('MC_SERVICE_UNIT', 'marketing-copilot.service'),
        help='systemd unit that must be inactive before maintenance',
    )
    parser.add_argument('--workers', type=int, default=int(os.environ.get('MC_VNEXT_WORKERS', '4')))
    parser.add_argument('--batch-size', type=int, default=512)
    parser.add_argument(
        '--threads',
        type=int,
        default=int(os.environ.get('MC_VNEXT_PROJECTION_MAINTENANCE_THREADS', '1')),
        help='DuckDB execution threads for offline maintenance; 1 is the conservative default',
    )
    parser.add_argument(
        '--staging-mode',
        choices=('vectorized', 'executemany'),
        default=os.environ.get('MC_VNEXT_PROJECTION_MAINTENANCE_STAGING_MODE', 'vectorized'),
    )
    parser.add_argument(
        '--no-reclaim',
        action='store_true',
        help='never remove acknowledged journal segments; required for a separate output root',
    )
    parser.add_argument(
        '--resume-output',
        action='store_true',
        help='resume an existing separate output root using its persisted journal offsets',
    )
    parser.add_argument(
        '--defer-task-index',
        action='store_true',
        help=(
            'rebuild the SQLite task-index sidecar once from the canonical '
            'DuckDB table after replay; useful for a fresh offline rebuild'
        ),
    )
    parser.add_argument(
        '--checkpoint-every-events',
        type=int,
        default=int(os.environ.get('MC_VNEXT_PROJECTION_MAINTENANCE_CHECKPOINT_EVENTS', '100000')),
        help='checkpoint at clean replay boundaries after this many new events (0 disables periodic checkpoints)',
    )
    parser.add_argument('--no-checkpoint', action='store_true', help='replay only; leave final WAL for a later maintenance window')
    parser.add_argument('--progress-seconds', type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    source_root = Path(args.root).expanduser().resolve()
    if (source_root / 'projection' / 'vnext-projection.sqlite3').exists():
        raise RuntimeError('legacy DuckDB maintenance cannot run on a migrated SQLite projection')
    root = (
        Path(args.output_root).expanduser().resolve()
        if args.output_root is not None
        else source_root
    )
    separate_output = root != source_root
    if separate_output and not args.no_reclaim:
        print(
            json.dumps(
                {
                    'ok': False,
                    'error': 'output_root_requires_no_reclaim',
                    'output_root': str(root),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2
    projection_root = root / 'projection'
    progress_root = source_root / 'progress'
    db_path = projection_root / 'vnext-projection.duckdb'
    offsets_path = projection_root / 'journal-offsets.json'
    task_index_path = projection_root / 'vnext-task-index.sqlite3'
    if _service_is_active(str(args.service_unit)):
        print(
            json.dumps(
                {'ok': False, 'error': 'service_active', 'service_unit': args.service_unit},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2
    projection_root.mkdir(parents=True, exist_ok=True)
    if separate_output:
        protected = (
            db_path,
            Path(f'{db_path}.wal'),
            task_index_path,
            Path(f'{task_index_path}-wal'),
            Path(f'{task_index_path}-shm'),
            offsets_path,
        )
        existing = [str(path) for path in protected if path.exists()]
        if existing and not args.resume_output:
            print(
                json.dumps(
                    {'ok': False, 'error': 'output_root_not_empty', 'paths': existing},
                    ensure_ascii=False,
                ),
                file=sys.stderr,
            )
            return 2
        if args.resume_output and (not db_path.exists() or not offsets_path.exists()):
            print(
                json.dumps(
                    {
                        'ok': False,
                        'error': 'resume_output_missing_state',
                        'required': [str(db_path), str(offsets_path)],
                    },
                    ensure_ascii=False,
                ),
                file=sys.stderr,
            )
            return 2
    elif not db_path.exists():
        print(
            json.dumps({'ok': False, 'error': 'projection_database_missing', 'path': str(db_path)}),
            file=sys.stderr,
        )
        return 2

    # DuckDB 1.2.2 is the minimum SLES12SP5 package version for the fixed
    # batch-insert constraint path used by the projection tables.
    # Keep offline batches at the same bounded size as the online writer;
    # larger batches have reproduced native failures during long replays.
    batch_size = max(1, min(512, int(args.batch_size)))
    journals = tuple(
        ProgressJournal(path)
        for path in _journal_paths(progress_root, max(1, int(args.workers)))
    )
    offsets = _load_offsets(offsets_path)
    before = _storage(journals, offsets)
    started = time.monotonic()
    last_report = started
    applied = 0
    duplicate = 0
    cursor = 0
    task_index_rebuilt = None
    checkpoint_every_events = max(0, int(args.checkpoint_every_events))
    next_checkpoint = checkpoint_every_events

    def checkpoint_at_clean_boundary() -> None:
        writer._batch_table_created = False
        for table in (
            '_vnext_projection_new',
            '_vnext_projection_batch',
            '_vnext_task_index_batch',
            '_vnext_retention_task_ids',
            '_vnext_retention_progress_ids',
        ):
            writer._conn.execute(f'DROP TABLE IF EXISTS {table}')
        writer._conn.execute('CHECKPOINT')

    writer = ProjectionWriter(
        db_path,
        batch_size=batch_size,
        staging_mode=str(args.staging_mode),
        task_index_path=task_index_path,
    )
    try:
        # Do not inherit an operator opt-in hot checkpoint setting into the
        # long replay. Checkpoints happen only after a committed, bounded
        # quantum, never inside the mixed staging transaction.
        writer._checkpoint_bytes = 0
        writer.start()
        writer._conn.execute(
            f"SET threads TO {max(1, min(8, int(args.threads)))}"
        )
        if args.defer_task_index and writer._task_index_conn is not None:
            writer._task_index_conn.close()
            writer._task_index_conn = None
        while True:
            events, candidates, cursor = _read_quantum(
                journals, offsets, cursor, batch_size
            )
            if not events:
                break
            for event in events:
                writer.submit(event)
            flushed = writer.flush(max_events=len(events))
            if flushed != len(events):
                raise RuntimeError(
                    f'offline replay flushed {flushed} of {len(events)} events'
                )
            applied += int(writer._last_batch_new)
            duplicate += int(writer._last_batch_duplicates)
            offsets.update(candidates)
            _persist_offsets(offsets_path, offsets)
            if not args.no_reclaim:
                removed = []
                for journal in journals:
                    removed.extend(journal.reclaim(offsets))
                if removed:
                    for path in removed:
                        offsets.pop(path, None)
                    _persist_offsets(offsets_path, offsets)
            while (
                not args.no_checkpoint
                and checkpoint_every_events > 0
                and applied >= next_checkpoint
            ):
                checkpoint_at_clean_boundary()
                next_checkpoint += checkpoint_every_events
            now = time.monotonic()
            if now - last_report >= max(0.5, float(args.progress_seconds)):
                current = _storage(journals, offsets)
                print(
                    json.dumps(
                        {
                            'phase': 'replay',
                            'applied': applied,
                            'duplicates': duplicate,
                            'backlog_bytes': current['backlog_bytes'],
                            'backlog_events_estimate': current['backlog_events_estimate'],
                            'elapsed_seconds': round(now - started, 1),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                last_report = now
        if not args.no_checkpoint:
            checkpoint_at_clean_boundary()
        if args.defer_task_index:
            # Close DuckDB before the sidecar bulk rebuild.  The canonical
            # task index is already in DuckDB, and rebuilding SQLite in one
            # offline pass avoids one SQLite WAL commit per replay quantum.
            writer.stop()
            task_index_rebuilt = rebuild_from_duckdb(
                db_path,
                task_index_path,
            )
        after = _storage(journals, offsets)
        remaining_backlog = int(after['backlog_bytes'])
        if remaining_backlog != 0:
            raise RuntimeError(f'offline replay left {remaining_backlog} journal bytes')
        result = {
            'ok': True,
            'db_path': str(db_path),
            'source_root': str(source_root),
            'output_root': str(root),
            'before': before,
            'after': after,
            'applied': applied,
            'duplicates': duplicate,
            'reclaim_enabled': not args.no_reclaim,
            'task_index_rebuilt': task_index_rebuilt,
            'checkpointed': not args.no_checkpoint,
            'elapsed_seconds': round(time.monotonic() - started, 1),
        }
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0
    except BaseException as exc:
        print(
            json.dumps(
                {
                    'ok': False,
                    'error': f'{type(exc).__name__}: {exc}',
                    'before': before,
                    'current': _storage(journals, offsets),
                    'applied': applied,
                    'duplicates': duplicate,
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
            flush=True,
        )
        return 1
    finally:
        writer.stop()


if __name__ == '__main__':
    raise SystemExit(main())
