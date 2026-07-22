"""Deterministic P2 failover, pooling, workers, metrics and alerts checks."""
import os
import tempfile
import threading
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class FakeConnection:
    instances = []
    def __init__(self, config, slot):
        self.config, self.slot = dict(config), slot
        self.connected, self.calls = False, 0
        self.__class__.instances.append(self)
    def connect(self):
        if self.config.get("fail_connect"): raise ConnectionError("unavailable")
        self.connected = True
    def disconnect(self): self.connected = False
    def send(self, task):
        self.calls += 1
        if self.config.get("fail_send"): raise ConnectionError("send failed")
        return [f"{self.config['id']}-{self.slot}-{self.calls}"]
    def status(self):
        return {"slot": self.slot, "state": "bound" if self.connected else "disconnected",
                "connected": self.connected, "last_error": "", "last_connected_at": None}


def config(config_id, priority, pool_size=1, **extra):
    return {"id": config_id, "name": f"smsc-{config_id}", "priority": priority,
            "pool_size": pool_size, "updated_at": "1", "reconnect_max_seconds": 1, **extra}


def main():
    root = Path(tempfile.mkdtemp(prefix="mc-p2-"))
    os.environ["DUCKDB_PATH"] = str(root / "p2.duckdb")
    os.environ["MC_TASK_WORKERS"] = "3"
    os.environ["MC_SYSTEM_MODE"] = "development"
    os.environ["MC_ALERT_QUEUE_DEPTH"] = "1"
    notifications = []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            notifications.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(204); self.end_headers()
        def log_message(self, *_): pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    os.environ["MC_ALERT_WEBHOOK_URL"] = f"http://127.0.0.1:{server.server_port}/alerts"
    from database import get_connection, init_db
    from services.smpp_gateway import SmppGateway, SmppDeliveryUnknownError
    from services import metrics_service, task_worker
    init_db()

    primary, backup = config(1, 10, fail_connect=True), config(2, 20, pool_size=2)
    gateway = SmppGateway(FakeConnection)
    gateway._enabled_configs = lambda: [primary, backup]
    gateway.connect()
    state = gateway.status()
    assert state["active_config_id"] == 2
    assert len(state["pools"][1]["connections"]) == 2
    results = [gateway.send({"id": n})[0] for n in range(4)]
    assert results == ["2-1-1", "2-2-1", "2-1-2", "2-2-2"], results

    unknown = SmppDeliveryUnknownError("unknown")
    assert unknown.status == -1
    task_worker.start_worker()
    assert task_worker.worker_status()["running"] == 3
    text = metrics_service.prometheus_text()
    assert "marketing_copilot_workers 3" in text
    task_worker.stop_worker()

    conn = get_connection()
    conn.execute("""INSERT INTO sms_tasks(campaign_type,campaign_id,campaign_name,msisdn,content,dedupe_key,status)
        VALUES ('realtime',1,'alert','251900000001','x','p2-alert','queued')""")
    conn.commit(); conn.close()
    alerts = metrics_service.evaluate_alerts(0, {"enabled_configs": 2, "connected": False})
    assert {item["code"] for item in alerts} == {"QUEUE_DEPTH_HIGH", "ALL_SMSC_DOWN"}
    conn = get_connection(); conn.execute("UPDATE sms_tasks SET status='delivered'"); conn.commit(); conn.close()
    metrics_service.evaluate_alerts(3, {"enabled_configs": 2, "connected": True})
    conn = get_connection()
    unresolved = conn.execute("SELECT COUNT(*) count FROM system_events WHERE category='capacity' AND resolved=0").fetchone()["count"]
    conn.close(); assert unresolved == 0
    server.shutdown(); server.server_close()
    assert {item["event"] for item in notifications} == {"firing", "resolved"}, notifications
    print("P2 CAPACITY PASS")


if __name__ == "__main__": main()
