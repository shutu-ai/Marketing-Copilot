import logging
import re
from pathlib import Path
from typing import Optional

from services import runtime_service

logger = logging.getLogger(__name__)

_LEVEL_ORDER = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}

# Matches lines emitted by the RotatingFileHandler formatter:
#   2026-08-04 16:05:48 [INFO] services.task_worker: message
_ENTRY_RE = re.compile(
    r"^(?P<time>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)\s+"
    r"\[(?P<level>[A-Z]+)\]\s+(?P<body>.*)$"
)

# Read at most the last 20 MB of a file so very large logs stay responsive.
MAX_READ_BYTES = 20 * 1024 * 1024


def get_log_dir() -> Path:
    return runtime_service.get_log_path().parent


def resolve_file(name: str) -> Optional[Path]:
    """Resolve *name* to an existing file inside the log directory, or None."""
    base = get_log_dir().resolve()
    candidate = (base / name).resolve()
    if candidate.parent != base or not candidate.is_file():
        return None
    return candidate


def list_log_files() -> list:
    """List current + rotated application log files, newest first."""
    primary = runtime_service.get_log_path().name
    base = get_log_dir()
    if not base.is_dir():
        return []
    files = []
    for path in sorted(base.iterdir()):
        if not path.is_file():
            continue
        name = path.name
        if name != primary and not name.startswith(primary + "."):
            continue
        stat = path.stat()
        files.append({
            "name": name,
            "path": str(path),
            "size": stat.st_size,
            "modified": stat.st_mtime,
        })
    # Primary file first, then rotated backups (newest backup first).
    files.sort(key=lambda item: (item["name"] != primary, -item["modified"]))
    return files


def _read_tail(path: Path, max_bytes: int = MAX_READ_BYTES) -> str:
    size = path.stat().st_size
    if size <= max_bytes:
        return path.read_text(encoding="utf-8", errors="replace")
    with path.open("rb") as fh:
        fh.seek(size - max_bytes)
        data = fh.read(max_bytes)
    text = data.decode("utf-8", errors="replace")
    newline = text.find("\n")
    return text[newline + 1:] if newline != -1 else text


def _parse_entries(text: str) -> list:
    entries = []
    current = None
    for line_no, line in enumerate(text.splitlines(), start=1):
        match = _ENTRY_RE.match(line)
        if match:
            if current:
                entries.append(current)
            body = match.group("body")
            logger_name, sep, message = body.partition(": ")
            current = {
                "time": match.group("time"),
                "level": match.group("level"),
                "logger": logger_name if sep else "",
                "message": message if sep else body,
                "line": line_no,
            }
        else:
            if current is None:
                current = {"time": "", "level": "", "logger": "", "message": line, "line": line_no}
            else:
                current["message"] += "\n" + line
    if current:
        entries.append(current)
    return entries


def _matches_filters(entry: dict, min_level: Optional[int], keyword: str) -> bool:
    if min_level is not None:
        level = entry.get("level", "")
        if _LEVEL_ORDER.get(level, 0) < min_level:
            return False
    if keyword:
        haystack = f"{entry.get('logger', '')} {entry.get('message', '')}".lower()
        if keyword.lower() not in haystack:
            return False
    return True


def read_entries(name: str, level: str = "", keyword: str = "", page: int = 1,
                 page_size: int = 100) -> dict:
    """Read paginated log entries from a log file, newest first."""
    path = resolve_file(name)
    if path is None:
        raise ValueError(f"log file not found: {name}")

    min_level = None
    if level:
        normalized = level.strip().upper()
        if normalized not in _LEVEL_ORDER:
            raise ValueError(f"invalid level: {level}")
        min_level = _LEVEL_ORDER[normalized]

    text = _read_tail(path)
    entries = _parse_entries(text)
    filtered = [entry for entry in entries if _matches_filters(entry, min_level, keyword.strip())]
    filtered.reverse()

    page = max(1, page)
    page_size = min(max(1, page_size), 500)
    total = len(filtered)
    start = (page - 1) * page_size
    items = filtered[start:start + page_size]
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": items,
        "truncated": path.stat().st_size > MAX_READ_BYTES,
    }
