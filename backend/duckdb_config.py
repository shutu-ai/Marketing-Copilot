"""Side-effect-free DuckDB settings shared by the app and test harness.

The application database owner adds its concurrency-specific settings in
``database.py``.  This module only resolves the extension directory and the
optional offline-only guard so a test tool can use the same extension lookup
without importing application state.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path


_OFFLINE_EXTENSION_LOCK = threading.Lock()
_OFFLINE_EXTENSION_READY: set[tuple[int, str]] = set()


def _is_truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def duckdb_extension_config(
    extension_directory: str | os.PathLike[str] | None = None,
    *,
    offline_only: bool | None = None,
) -> dict[str, str]:
    """Return connection-local extension settings without opening DuckDB.

    ``autoinstall_known_extensions`` is deliberately opt-in through
    ``MC_DUCKDB_OFFLINE_ONLY``.  Normal production connections retain their
    existing DuckDB behavior; the isolated Runner sets the flag only for its
    child processes and its post-shutdown audit connections.
    """

    configured = str(
        extension_directory
        if extension_directory is not None
        else os.environ.get("MC_DUCKDB_EXTENSION_DIR", "")
    ).strip()
    if not configured:
        configured = str(Path(__file__).resolve().parents[1] / "offline" / "extensions")

    config: dict[str, str] = {}
    directory = Path(configured).expanduser()
    if directory.is_dir():
        config["extension_directory"] = str(directory.resolve())

    if offline_only is None:
        offline_only = _is_truthy(os.environ.get("MC_DUCKDB_OFFLINE_ONLY", ""))
    if offline_only:
        # Keep autoload_known_extensions at DuckDB's default so a matching
        # packaged core_functions extension can be loaded during WAL replay,
        # while forbidding the fallback download from the public repository.
        config["autoinstall_known_extensions"] = "false"

    return config


def _sql_string(value: str) -> str:
    """Return a SQL string literal for a local filesystem path."""

    return "'" + str(value).replace("'", "''") + "'"


def _core_functions_path(duckdb_module, extension_directory: str) -> Path:
    directory = Path(extension_directory).expanduser().resolve()
    version = str(getattr(duckdb_module, "__version__", ""))
    candidates = sorted(
        item
        for item in directory.rglob("core_functions.duckdb_extension")
        if item.is_file() and not item.is_symlink() and version in item.parts
    )
    if not candidates:
        raise RuntimeError(
            "OFFLINE_EXTENSION_MISSING: no matching core_functions.duckdb_extension "
            f"for DuckDB {version} under {directory}"
        )
    return candidates[0]


def prepare_offline_extensions(
    duckdb_module, config: dict[str, str]
) -> dict[str, str] | None:
    """Create local DuckDB install metadata before opening an existing DB.

    DuckDB 1.2.0 WAL replay may request ``core_functions`` during the native
    ``connect(database, read_only=True)`` call, before Python can issue a
    ``LOAD`` on that database connection. A bare extension binary is not
    enough for that path: ``INSTALL '<local-file>'`` creates the matching
    ``.info`` metadata in the connection's extension directory. The install
    source is an explicit local path, so this function never contacts a
    repository or downloads anything.
    """

    if config.get("autoinstall_known_extensions") != "false":
        return None
    extension_directory = str(config.get("extension_directory") or "").strip()
    if not extension_directory:
        raise RuntimeError(
            "OFFLINE_EXTENSION_MISSING: offline mode has no extension_directory"
        )
    core_path = _core_functions_path(duckdb_module, extension_directory)
    key = (id(duckdb_module), extension_directory)
    with _OFFLINE_EXTENSION_LOCK:
        info_paths = sorted(
            item
            for item in Path(extension_directory).rglob(
                "core_functions.duckdb_extension.info"
            )
            if item.is_file() and not item.is_symlink()
        )
        if key not in _OFFLINE_EXTENSION_READY or not info_paths:
            memory = duckdb_module.connect(":memory:", config=dict(config))
            try:
                if not info_paths:
                    memory.execute("INSTALL " + _sql_string(str(core_path)))
                # LOAD is connection-local for normal extensions and is also
                # harmless for DuckDB's statically linked core_functions.
                memory.execute("LOAD core_functions")
            finally:
                memory.close()
            info_paths = sorted(
                item
                for item in Path(extension_directory).rglob(
                    "core_functions.duckdb_extension.info"
                )
                if item.is_file() and not item.is_symlink()
            )
            if not info_paths:
                raise RuntimeError(
                    "OFFLINE_EXTENSION_MISSING: DuckDB did not create install "
                    f"metadata for {core_path}"
                )
            _OFFLINE_EXTENSION_READY.add(key)
        info_path = info_paths[0]
    return {
        "core_functions": str(core_path),
        "info": str(info_path),
    }
