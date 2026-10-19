"""Fast, dependency-light tests for the local acceptance harness.

These tests do not replace the real-process acceptance runs.  They protect the
producer file contract and the deterministic SMSC fault decisions used by
those runs.
"""

from __future__ import annotations

import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path


TOOLS_DIR = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS_DIR))

from local_e2e_data import write_event_file  # noqa: E402
from smsc_simulator import SmscSimulator  # noqa: E402


class LocalAcceptanceToolTests(unittest.TestCase):
    def test_producer_publishes_part_ready_atomically(self):
        root = Path.cwd() / f".local-e2e-tool-test-{uuid.uuid4().hex}"
        try:
            root.mkdir(parents=True)
            manifest = write_event_file(
                root,
                "E2E_",
                1,
                [{"msisdn": "2511000001", "language": "English", "event_id": "t-1"}],
                ready=True,
            )
            final_path = Path(manifest["path"])
            self.assertTrue(final_path.exists())
            self.assertTrue(final_path.with_name(final_path.name + ".ready").exists())
            self.assertFalse(final_path.with_suffix(final_path.suffix + ".part").exists())
            self.assertEqual(manifest["row_count"], 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_smsc_fault_profile_is_deterministic(self):
        reject = SmscSimulator("127.0.0.1", 0, "test", "test", 0, reject_ratio=1.0)
        self.assertEqual(reject.response_status(1), 0x0000000B)

        partial = SmscSimulator(
            "127.0.0.1", 0, "test", "test", 0,
            partial_submit_after=1,
            partial_submit_status=0x0000000B,
        )
        self.assertEqual(partial.response_status(1), 0)
        self.assertEqual(partial.response_status(2), 0x0000000B)

        dropped = SmscSimulator("127.0.0.1", 0, "test", "test", 0, drop_response_ratio=1.0)
        self.assertTrue(dropped.should_drop_response(1))
        self.assertFalse(dropped.should_drop_receipt(1))

        disconnected = SmscSimulator("127.0.0.1", 0, "test", "test", 0, disconnect_after=2)
        self.assertFalse(disconnected.should_disconnect(1))
        self.assertTrue(disconnected.should_disconnect(2))

        ambiguous = SmscSimulator(
            "127.0.0.1", 0, "test", "test", 0,
            disconnect_before_response=3,
        )
        self.assertFalse(ambiguous.should_disconnect_before_response(2))
        self.assertTrue(ambiguous.should_disconnect_before_response(3))

    def test_smsc_event_log_is_machine_readable(self):
        root = Path.cwd() / f".local-e2e-tool-test-{uuid.uuid4().hex}"
        try:
            root.mkdir(parents=True)
            event_log = root / "events.jsonl"
            simulator = SmscSimulator("127.0.0.1", 0, "test", "test", 0, event_log=str(event_log))
            simulator.record_event({"event_type": "submit_sm", "response_status": 0})
            records = [json.loads(line) for line in event_log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(records[0]["event_type"], "submit_sm")
            self.assertEqual(records[0]["response_status"], 0)
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
