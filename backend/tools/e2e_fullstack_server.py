"""Isolated development-mode API used by the full-stack Playwright test."""

import argparse
import os
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8014)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="marketing-copilot-fullstack-") as temp_name:
        root = Path(temp_name)
        os.environ.update({
            "DUCKDB_PATH": str(root / "e2e.duckdb"),
            "MC_BACKUP_DIR": str(root / "backups"),
            "MC_FRONTEND_DIR": str(root / "missing-dist"),
            "MC_INSTANCE_LOCK": str(root / "instance.lock"),
            "MC_LOG_PATH": str(root / "e2e.log"),
            "MC_SYSTEM_MODE": "development",
        })
        import uvicorn
        uvicorn.run("main:app", host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
