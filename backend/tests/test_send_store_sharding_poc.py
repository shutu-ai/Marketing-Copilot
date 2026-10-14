import json

import pytest

from services.send_store_sharding_poc import (
    SendStoreDisabled,
    SendStoreFenceError,
    ShardedSendStore,
    stable_shard_id,
)


def test_sharded_send_store_is_opt_in_and_mapping_is_restart_stable(tmp_path, monkeypatch):
    monkeypatch.setenv("SHARDED_SEND_STORE", "OFF")
    store = ShardedSendStore(tmp_path / "off", 1)
    with pytest.raises(SendStoreDisabled):
        store.shard_for("realtime", 1)
    store.close()

    root = tmp_path / "on"
    first = ShardedSendStore(root, 4, enabled=True)
    first_map = [first.shard_for("realtime", campaign_id) for campaign_id in range(1, 8)]
    first.close()
    second = ShardedSendStore(root, 4, enabled=True)
    second_map = [second.shard_for("realtime", campaign_id) for campaign_id in range(1, 8)]
    assert second_map == first_map
    assert first_map == [stable_shard_id("realtime", campaign_id, 4) for campaign_id in range(1, 8)]
    assert json.loads((root / "shard-map.json").read_text(encoding="utf-8"))["shard_count"] == 4
    second.close()


def test_pause_cancel_resume_and_unknown_are_fenced(tmp_path):
    store = ShardedSendStore(tmp_path, 2, enabled=True)
    try:
        store.register_campaign("realtime", 10)
        store.register_campaign("realtime", 20)
        store.enqueue("realtime", 10, ({"ordinal": index, "planned_parts": 2} for index in range(4)))
        store.enqueue("realtime", 20, ({"ordinal": index} for index in range(4)))

        claimed = store.claim(1)
        task = claimed[0]
        store.transition("realtime", 10, "pause")
        with pytest.raises(SendStoreFenceError):
            store.authorize(task["task_key"], task["claim_token"], planned_parts=2)

        other = store.claim(10)
        assert other and {int(item["campaign_id"]) for item in other} == {20}
        attempt = store.authorize(other[0]["task_key"], other[0]["claim_token"], planned_parts=2)
        store.append_result(attempt, 1, "success", smsc_message_id="ok-1")
        store.append_result(attempt, 2, "unknown", smsc_message_id="")

        store.transition("realtime", 10, "resume")
        resumed = store.claim(1)
        assert resumed and int(resumed[0]["campaign_id"]) == 10
        store.transition("realtime", 10, "cancel")
        check = store.consistency_check()
        assert check["ok"] is True
        row = store._shards[store.shard_for("realtime", 20)].conn.execute(
            "SELECT status FROM tasks WHERE campaign_id=20 ORDER BY ordinal LIMIT 1"
        ).fetchone()
        assert row["status"] == "partial"
    finally:
        store.close()


def test_result_journal_replay_is_idempotent(tmp_path):
    store = ShardedSendStore(tmp_path, 1, enabled=True)
    try:
        store.register_campaign("batch", 1)
        store.enqueue("batch", 1, [{"ordinal": 0, "planned_parts": 1}])
        task = store.claim(1)[0]
        attempt = store.authorize(task["task_key"], task["claim_token"], planned_parts=1)
        event_id = store.append_result(attempt, 1, "success", apply=False)
        assert store.replay_journal() == 1
        assert store.replay_journal() == 0
        assert store.consistency_check()["ok"] is True
        row = store._shards[0].conn.execute(
            "SELECT status FROM tasks WHERE task_key=?", (task["task_key"],)
        ).fetchone()
        assert row["status"] == "accepted"
        assert event_id
    finally:
        store.close()
