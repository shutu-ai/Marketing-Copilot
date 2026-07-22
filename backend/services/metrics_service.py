import json
import os
import threading
import time
import urllib.request
from collections import defaultdict

from database import get_connection

_lock = threading.Lock()
_counters = defaultdict(float)
_gauges = {}
_started_at = time.time()


def _key(name: str, labels=None):
    return name, tuple(sorted((str(k), str(v)) for k, v in (labels or {}).items()))


def inc(name: str, amount: float = 1, labels=None):
    with _lock:
        _counters[_key(name, labels)] += amount


def set_gauge(name: str, value: float, labels=None):
    with _lock:
        _gauges[_key(name, labels)] = float(value)


def observe_runtime_state(worker_count=0):
    conn = get_connection()
    rows = conn.execute("SELECT status,COUNT(*) count FROM sms_tasks GROUP BY status").fetchall()
    conn.close()
    counts = {row["status"]: row["count"] for row in rows}
    for status in ("queued", "retrying", "sending", "submitted", "delivered", "failed", "expired"):
        set_gauge("marketing_copilot_tasks", counts.get(status, 0), {"status": status})
    set_gauge("marketing_copilot_queue_depth", counts.get("queued", 0) + counts.get("retrying", 0))
    set_gauge("marketing_copilot_workers", worker_count)
    set_gauge("marketing_copilot_uptime_seconds", time.time() - _started_at)
    return counts


def snapshot() -> dict:
    with _lock:
        counters = [{"name": name, "labels": dict(labels), "value": value}
                    for (name, labels), value in _counters.items()]
        gauges = [{"name": name, "labels": dict(labels), "value": value}
                  for (name, labels), value in _gauges.items()]
    return {"counters": counters, "gauges": gauges, "uptime_seconds": round(time.time() - _started_at, 3)}


def _format_labels(labels):
    if not labels:
        return ""
    escaped = [f'{key}="{value.replace(chr(92), chr(92)+chr(92)).replace(chr(34), chr(92)+chr(34))}"'
               for key, value in labels]
    return "{" + ",".join(escaped) + "}"


def prometheus_text() -> str:
    from services.task_worker import worker_status
    observe_runtime_state(worker_status()["running"])
    lines = ["# Marketing Copilot runtime metrics"]
    with _lock:
        values = list(_counters.items()) + list(_gauges.items())
    for (name, labels), value in sorted(values, key=lambda item: (item[0][0], item[0][1])):
        lines.append(f"{name}{_format_labels(labels)} {value:g}")
    return "\n".join(lines) + "\n"


def _notify_alert(event: str, code: str, message: str, severity: str, context=None):
    url = os.environ.get("MC_ALERT_WEBHOOK_URL", "").strip()
    if not url:
        return
    payload = json.dumps({"event": event, "code": code, "message": message,
                          "severity": severity, "context": context or {},
                          "timestamp": time.time()}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=float(os.environ.get("MC_ALERT_WEBHOOK_TIMEOUT", "5"))) as response:
            response.read(1)
        inc("marketing_copilot_alert_notifications_total", labels={"event": event, "result": "success"})
    except Exception:
        inc("marketing_copilot_alert_notifications_total", labels={"event": event, "result": "failure"})


def _open_alert(code: str, message: str, severity: str = "warning", context=None):
    conn = get_connection()
    existing = conn.execute("SELECT id FROM system_events WHERE code=? AND resolved=0 LIMIT 1", (code,)).fetchone()
    if not existing:
        conn.execute("INSERT INTO system_events(category,severity,code,message,context) VALUES ('capacity',?,?,?,?,?)".replace("?,?,?,?,?", "?,?,?,?"),
                     (severity, code, message, json.dumps(context or {}, ensure_ascii=False)))
        conn.commit()
        inc("marketing_copilot_alerts_total", labels={"code": code, "severity": severity})
        _notify_alert("firing", code, message, severity, context)
    conn.close()


def evaluate_alerts(worker_count: int = 0, gateway_status=None) -> list:
    counts = observe_runtime_state(worker_count)
    alerts = []
    queue_depth = counts.get("queued", 0) + counts.get("retrying", 0)
    queue_limit = int(os.environ.get("MC_ALERT_QUEUE_DEPTH", "1000"))
    if queue_depth >= queue_limit:
        alerts.append(("QUEUE_DEPTH_HIGH", f"SMS queue depth is {queue_depth} (threshold {queue_limit})", {"queue_depth": queue_depth}))
    terminal = counts.get("delivered", 0) + counts.get("failed", 0)
    failure_rate = counts.get("failed", 0) / terminal if terminal else 0
    rate_limit = float(os.environ.get("MC_ALERT_FAILURE_RATE", "0.2"))
    if terminal >= 20 and failure_rate >= rate_limit:
        alerts.append(("FAILURE_RATE_HIGH", f"SMS failure rate is {failure_rate:.1%} (threshold {rate_limit:.1%})", {"failure_rate": failure_rate}))
    if gateway_status and gateway_status.get("enabled_configs", 0) and not gateway_status.get("connected"):
        alerts.append(("ALL_SMSC_DOWN", "All enabled SMSC connections are unavailable", gateway_status))
    for code, message, context in alerts:
        _open_alert(code, message, "error" if code == "ALL_SMSC_DOWN" else "warning", context)
    conn = get_connection()
    previous = {row["code"]: dict(row) for row in conn.execute(
        "SELECT code,message,severity,context FROM system_events WHERE category='capacity' AND resolved=0").fetchall()}
    active_codes = [code for code, _, _ in alerts]
    if active_codes:
        placeholders = ",".join("?" for _ in active_codes)
        conn.execute(f"UPDATE system_events SET resolved=1 WHERE category='capacity' AND resolved=0 AND code NOT IN ({placeholders})", active_codes)
    else:
        conn.execute("UPDATE system_events SET resolved=1 WHERE category='capacity' AND resolved=0")
    conn.commit(); conn.close()
    for code in previous.keys() - set(active_codes):
        item = previous[code]
        try: context = json.loads(item.get("context") or "{}")
        except json.JSONDecodeError: context = {}
        _notify_alert("resolved", code, item["message"], item["severity"], context)
    return [{"code": code, "message": message, "context": context} for code, message, context in alerts]
