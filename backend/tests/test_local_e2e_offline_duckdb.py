"""Offline DuckDB/WAL guard tests for the isolated acceptance Runner."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


BACKEND_DIR = Path(__file__).resolve().parents[1]
TOOLS_DIR = BACKEND_DIR / "tools"
sys.path.insert(0, str(BACKEND_DIR))
sys.path.insert(0, str(TOOLS_DIR))

from duckdb_config import (  # noqa: E402
    duckdb_extension_config,
    prepare_offline_extensions,
)
from database import duckdb_connection_config  # noqa: E402
from local_e2e_acceptance import BlockedEnv, _stage_offline_extensions  # noqa: E402


class OfflineDuckDBConfigTests(unittest.TestCase):
    def test_offline_mode_disables_install_but_keeps_extension_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            extension_dir = Path(temporary)
            with patch.dict(
                os.environ,
                {
                    "MC_DUCKDB_EXTENSION_DIR": str(extension_dir),
                    "MC_DUCKDB_OFFLINE_ONLY": "1",
                },
                clear=False,
            ):
                config = duckdb_extension_config(extension_dir, offline_only=True)
                self.assertEqual(config["extension_directory"], str(extension_dir.resolve()))
                self.assertEqual(config["autoinstall_known_extensions"], "false")

                app_config = duckdb_connection_config()
                self.assertEqual(app_config, config)

    def test_missing_core_functions_fails_closed(self):
        with tempfile.TemporaryDirectory() as source, tempfile.TemporaryDirectory() as target:
            with self.assertRaisesRegex(BlockedEnv, "OFFLINE_EXTENSION_MISSING"):
                _stage_offline_extensions(Path(source), Path(target) / "extensions")

    def test_offline_prepare_requires_a_matching_extension(self):
        fake_duckdb = type("FakeDuckDB", (), {"__version__": "1.2.0"})
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(RuntimeError, "OFFLINE_EXTENSION_MISSING"):
                prepare_offline_extensions(
                    fake_duckdb,
                    {
                        "extension_directory": temporary,
                        "autoinstall_known_extensions": "false",
                    },
                )

    def test_extension_copy_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as source, tempfile.TemporaryDirectory() as target:
            source_path = Path(source)
            (source_path / "1.2.0" / "linux_amd64_gcc4").mkdir(parents=True)
            extension = source_path / "1.2.0" / "linux_amd64_gcc4" / "core_functions.duckdb_extension"
            extension.write_bytes(b"test-extension")
            link = source_path / "link"
            try:
                link.symlink_to(extension)
            except (OSError, NotImplementedError):
                self.skipTest("symlink creation is unavailable on this host")
            with self.assertRaisesRegex(BlockedEnv, "symlink"):
                _stage_offline_extensions(source_path, Path(target) / "extensions")


if __name__ == "__main__":
    unittest.main()
