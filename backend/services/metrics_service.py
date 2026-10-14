import json
import os
import threading
import time
import urllib.request
from collections import defaultdict, deque

from database import get_connection, read_transaction

_lock = threading.Lock()
_counters = defaultdict(float)
_gauges = {}
_events = defaultdict(deque)
_started_at = time.time()
_EVENT_RETENTION_SECONDS = 300.0
_runtime_observe_lock = threading.Lock()
_last_runtime_observe_at = 0.0
_last_runtime_counts = {}


def _key(name: str, labels=None):
    return name, tuple(sorted((str(k), str(v)) for k, v in (labels or {}).items()))


def inc(name: str, amount: float = 1, labels=None):
    with _lock:
        _counters[_key(name, labels)] += amount


def _record_event_locked(name: str, amount: float, labels, now: float) -> None:
    events = _events[_key(name, labels)]
    events.append((now, float(amount)))
    cutoff = now - _EVENT_RETENTION_SECONDS
    while events and events[0][0] < cutoff:
        events.popleft()


def record_event(name: str, amount: float = 1, labels=None) -> None:
    """Record an event for cheap in-process rolling-rate measurements.

    Counters are intentionally kept separately from this short-lived event
    history: counters provide lifetime totals, while the event history makes
    it possible to report the actual transport rate without another DuckDB
    query on the hot path.  Five minutes is enough for the current 1-minute
    operational windows and bounds memory use under high throughput.
    """
    now = time.monotonic()
    with _lock:
        _record_event_locked(name, amount, labels, now)


def record_smpp_response(config_id, slot, status: int, result: str,
                         latency_seconds=None) -> None:
    """Record one submit_sm_resp without any database or log I/O.

    The response hot path uses one in-process lock acquisition for the normal
    result counter, status-code counter, rolling-rate event and optional
    latency observation.  Status is deliberately a bounded hexadecimal code
    label so operators can distinguish explicit SMSC errors such as
    ESME_RTHROTTLED without logging every successful response.
    """
    status = int(status or 0)
    result = str(result or "unknown")
    base_labels = {"config_id": config_id, "slot": slot}
    status_labels = {
        **base_labels,
        "result": result,
        "status": f"0x{status:08x}",
    }
    now = time.monotonic()
    with _lock:
        _counters[_key("marketing_copilot_smpp_submit_sm_resp_total",
                       {**base_labels, "result": result})] += 1
        _counters[_key("marketing_copilot_smpp_submit_sm_resp_status_total",
                       status_labels)] += 1
        _record_event_locked(
            "marketing_copilot_smpp_submit_sm_resp",
            1,
            {**base_labels, "result": result},
            now,
        )
        if latency_seconds is not None:
            latency = max(0.0, float(latency_seconds))
            latency_key = _key(
                "marketing_copilot_smpp_submit_sm_resp_latency_seconds",
                base_labels,
            )
            _gauges[latency_key] = latency
            _observe_histogram_locked(
                "marketing_copilot_smpp_submit_sm_resp_latency_seconds",
                latency, base_labels,
            )


def rolling_rate(name: str, labels=None, window_seconds: float = 60.0) -> float:
    """Return events per second over the requested recent time window."""
    window = max(0.1, float(window_seconds))
    now = time.monotonic()
    key = _key(name, labels)
    with _lock:
        events = _events.get(key)
        if not events:
            return 0.0
        cutoff = now - window
        while events and events[0][0] < now - _EVENT_RETENTION_SECONDS:
            events.popleft()
        total = sum(amount for timestamp, amount in events if timestamp >= cutoff)
    return total / window


def rolling_count(name: str, labels=None, window_seconds: float = 60.0) -> float:
    """Return the number of recent events in the requested time window."""
    return rolling_rate(name, labels, window_seconds) * max(0.1, float(window_seconds))


def set_gauge(name: str, value: float, labels=None):
    with _lock:
        _gauges[_key(name, labels)] = float(value)


def observe(name: str, value: float, labels=None):
    """Record the latest value plus count/sum series for a runtime observation."""
    value = float(value)
    with _lock:
        _gauges[_key(name, labels)] = value
        _counters[_key(f"{name}_count", labels)] += 1
        _counters[_key(f"{name}_sum", labels)] += value


def observe_histogram(name: str, value: float, labels=None, buckets=None):
    """Record a bounded cumulative histogram for latency/size measurements."""
    value = max(0.0, float(value))
    limits = tuple(buckets or (0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0))
    base_labels = dict(labels or {})
    with _lock:
        _observe_histogram_locked(name, value, base_labels, limits)


def _observe_histogram_locked(name: str, value: float, labels=None, buckets=None):
    """Update a histogram while the metrics lock is already held."""
    value = max(0.0, float(value))
    base_labels = dict(labels or {})
    limits = tuple(buckets or (0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0))
    _gauges[_key(f"{name}_latest", base_labels)] = value
    _counters[_key(f"{name}_count", base_labels)] += 1
    _counters[_key(f"{name}_sum", base_labels)] += value
    for limit in limits:
        if value <= limit:
            bucket_labels = {**base_labels, "le": str(limit)}
            _counters[_key(f"{name}_bucket", bucket_labels)] += 1
    _counters[_key(f"{name}_bucket", {**base_labels, "le": "+Inf"})] += 1


def observe_runtime_state(worker_count=0, allow_cached: bool = False):
    global _last_runtime_observe_at, _last_runtime_counts
    now = time.monotonic()
    # CapacityCenter, Prometheus and alert evaluation can poll at the same
    # instant.  One refresh per second is enough operationally and prevents
    # those readers from competing with DuckDB's single-writer workload.
    with _runtime_observe_lock:
        if allow_cached and now - _last_runtime_observe_at < 1.0:
            return dict(_last_runtime_counts)
        _last_runtime_observe_at = now
    rows = read_transaction(
        lambda conn: conn.execute(
            "SELECT status,COUNT(*) count FROM sms_tasks GROUP BY status"
        ).fetchall(),
        operation="metrics.task_status",
    )
    counts = {row["status"]: row["count"] for row in rows}
    for status in (
        "queued", "retrying", "sending", "submitted", "accepted", "delivered",
        "failed", "partial", "unknown", "expired",
    ):
        set_gauge("marketing_copilot_tasks", counts.get(status, 0), {"status": status})
    set_gauge("marketing_copilot_queue_depth", counts.get("queued", 0) + counts.get("retrying", 0))
    set_gauge("marketing_copilot_workers", worker_count)
    set_gauge("marketing_copilot_uptime_seconds", time.time() - _started_at)
    try:
        from services import sms_write_buffer
        buffered = sms_write_buffer.status()
        set_gauge("marketing_copilot_sms_write_queue_depth", buffered["queue_depth"])
        for queue_name, depth in buffered.get("queue_depths", {}).items():
            set_gauge("marketing_copilot_sms_write_queue_depth_by_kind", depth,
                      {"queue": queue_name})
    except Exception:
        pass
    try:
        from services import backpressure_service
        pressure = backpressure_service.snapshot(include_task_depth=False)
        set_gauge("marketing_copilot_ingestion_chunk_backlog", pressure["chunk_depth"])
        set_gauge("marketing_copilot_disk_used_percent", pressure["disk"]["used_pct"])
        set_gauge("marketing_copilot_disk_free_bytes", pressure["disk"]["free_bytes"])
        ramp = pressure.get("ingestion_ramp", {})
        set_gauge("marketing_copilot_realtime_ingestion_ramp_rate_tps", ramp.get("rate_tps", 0))
        set_gauge("marketing_copilot_realtime_ingestion_ramp_tokens", ramp.get("tokens", 0))
        set_gauge("marketing_copilot_realtime_ingestion_ramp_throttled_total", ramp.get("throttled", 0))
    except Exception:
        pass
    try:
        from database import write_lock_status
        lock = write_lock_status()
        set_gauge("marketing_copilot_database_write_lock_held", 1 if lock["held"] else 0)
        set_gauge("marketing_copilot_database_write_lock_age_seconds", lock["age_seconds"])
    except Exception:
        pass
    try:
        file_rows, phase_rows, active_phase_rows = read_transaction(
            lambda conn: (
                conn.execute(
                    "SELECT status,COUNT(*) count FROM ingestion_files WHERE campaign_type='realtime' GROUP BY status"
                ).fetchall(),
                conn.execute(
                    "SELECT processing_phase,COUNT(*) count FROM ingestion_files WHERE campaign_type='realtime' GROUP BY processing_phase"
                ).fetchall(),
                conn.execute(
                    """SELECT processing_phase,COUNT(*) count FROM ingestion_files
                       WHERE campaign_type='realtime' AND status='processing' GROUP BY processing_phase"""
                ).fetchall(),
            ),
            operation="metrics.ingestion_files",
        )
        counts_by_status = {row["status"]: int(row["count"] or 0) for row in file_rows}
        for status in ("pending", "processing", "retrying", "completed", "failed", "quarantined"):
            set_gauge("realtime_inbox_files", counts_by_status.get(status, 0), {"status": status})
        # Replace entire label families so phases disappearing from the query
        # cannot remain as stale gauges. Historical and active counts differ.
        with _lock:
            for key in list(_gauges):
                if key[0] in ("realtime_inbox_phase_files", "realtime_inbox_active_phase_files"):
                    _gauges[key] = 0.0
            for name, phases in (("realtime_inbox_phase_files", phase_rows),
                                 ("realtime_inbox_active_phase_files", active_phase_rows)):
                for phase in phases:
                    _gauges[_key(name, {"phase": phase["processing_phase"] or ""})] = float(phase["count"] or 0)
        pending = counts_by_status.get("pending", 0) + counts_by_status.get("retrying", 0)
        set_gauge("realtime_inbox_pending_files", pending)
        set_gauge("realtime_inbox_processing_files", counts_by_status.get("processing", 0))
        ready_rows, progress_row = read_transaction(
            lambda conn: (
                conn.execute(
                    "SELECT COALESCE(SUM(normalized_count),0) rows FROM realtime_whitelist_versions WHERE status='ready'"
                ).fetchone()["rows"],
                conn.execute(
                    """SELECT MAX(last_progress_at) last_progress_at FROM ingestion_files
                       WHERE campaign_type='realtime' AND status='processing'"""
                ).fetchone(),
            ),
            operation="metrics.ingestion_progress",
        )
        set_gauge("realtime_whitelist_index_rows", int(ready_rows or 0))
        if progress_row and progress_row["last_progress_at"]:
            try:
                set_gauge(
                    "realtime_ingestion_worker_last_progress_timestamp",
                    time.mktime(time.strptime(str(progress_row["last_progress_at"])[:19], "%Y-%m-%d %H:%M:%S")),
                )
            except (TypeError, ValueError, OverflowError):
                pass
    except Exception:
        pass
    # Per-SMSC queue depth, using the same attribution as the gateway:
    # NULL smpp_config_id tasks route to the first enabled config.
    config_rows, task_rows = read_transaction(
        lambda conn: (
            conn.execute(
                "SELECT id FROM smpp_configs WHERE enabled=1 ORDER BY priority,id"
            ).fetchall(),
            conn.execute(
                "SELECT smpp_config_id, status, COUNT(*) cnt FROM sms_tasks GROUP BY smpp_config_id, status"
            ).fetchall(),
        ),
        operation="metrics.smpp_queue",
    )
    default_id = config_rows[0]["id"] if config_rows else None
    by_config = defaultdict(int)
    for row in task_rows:
        if row["status"] not in ("queued", "retrying"):
            continue
        cfg_id = row["smpp_config_id"] if row["smpp_config_id"] is not None else default_id
        if cfg_id is None:
            continue
        by_config[cfg_id] += int(row["cnt"] or 0)
    for row in config_rows:
        cfg_id = row["id"]
        set_gauge("marketing_copilot_smpp_queue_size", by_config.get(cfg_id, 0),
                  {"config_id": cfg_id})
    with _runtime_observe_lock:
        _last_runtime_counts = dict(counts)
    return counts


def campaign_transport_snapshot(window_seconds: float = 60.0) -> dict:
    """Return in-memory per-campaign transport totals and recent rates.

    This intentionally performs no database I/O.  It is safe to include in
    the operational capacity endpoint even while DuckDB is busy.
    """
    window = max(1.0, min(_EVENT_RETENTION_SECONDS, float(window_seconds)))
    now = time.monotonic()
    cutoff = now - window
    result = defaultdict(lambda: {
        "scheduled_parts_total": 0.0,
        "socket_parts_total": 0.0,
        "socket_parts_1m": 0.0,
        "submit_sm_resp_success_total": 0.0,
        "submit_sm_resp_failure_total": 0.0,
        "submit_sm_resp_success_1m": 0.0,
        "submit_sm_resp_failure_1m": 0.0,
    })

    def campaign_key(labels_tuple):
        labels = dict(labels_tuple)
        campaign_type = labels.get("campaign_type", "")
        campaign_id = labels.get("campaign_id", "0")
        config_id = labels.get("config_id", labels.get("smpp_config_id", "0"))
        try:
            return campaign_type, int(campaign_id), int(config_id)
        except (TypeError, ValueError):
            return None

    with _lock:
        counters = list(_counters.items())
        events = [(key, list(values)) for key, values in _events.items()]
    for (name, labels), value in counters:
        key = campaign_key(labels)
        if key is None:
            continue
        if name == "marketing_copilot_campaign_scheduled_parts_total":
            result[key]["scheduled_parts_total"] += float(value)
        elif name == "marketing_copilot_campaign_socket_parts_total":
            result[key]["socket_parts_total"] += float(value)
        elif name == "marketing_copilot_campaign_submit_sm_resp_total":
            outcome = dict(labels).get("result", "unknown")
            field = ("submit_sm_resp_success_total" if outcome == "success"
                     else "submit_sm_resp_failure_total")
            result[key][field] += float(value)
    for (name, labels), values in events:
        key = campaign_key(labels)
        if key is None:
            continue
        recent = sum(amount for timestamp, amount in values if timestamp >= cutoff)
        normalized = recent * 60.0 / window
        if name == "marketing_copilot_campaign_socket_parts":
            result[key]["socket_parts_1m"] += normalized
        elif name == "marketing_copilot_campaign_submit_sm_resp":
            outcome = dict(labels).get("result", "unknown")
            field = ("submit_sm_resp_success_1m" if outcome == "success"
                     else "submit_sm_resp_failure_1m")
            result[key][field] += normalized
    return {key: dict(value) for key, value in result.items()}


def snapshot() -> dict:
    with _lock:
        counters = [{"name": name, "labels": dict(labels), "value": value}
                    for (name, labels), value in _counters.items()]
        gauges = [{"name": name, "labels": dict(labels), "value": value}
                  for (name, labels), value in _gauges.items()]
        rate_keys = list(_events.keys())
    rates = []
    for name, labels in rate_keys:
        rates.append({
            "name": f"{name}_per_second_1m",
            "labels": dict(labels),
            "value": rolling_rate(name, dict(labels), 60.0),
        })
    return {"counters": counters, "gauges": gauges, "rates": rates,
            "uptime_seconds": round(time.time() - _started_at, 3)}


def _format_labels(labels):
    if not labels:
        return ""
    escaped = [f'{key}="{value.replace(chr(92), chr(92)+chr(92)).replace(chr(34), chr(92)+chr(34))}"'
               for key, value in labels]
    return "{" + ",".join(escaped) + "}"


def prometheus_text() -> str:
    from services.task_worker import worker_status
    observe_runtime_state(worker_status()["running"], allow_cached=True)
    lines = ["# Marketing Copilot runtime metrics"]
    with _lock:
        values = list(_counters.items()) + list(_gauges.items())
    for (name, labels), value in sorted(values, key=lambda item: (item[0][0], item[0][1])):
        lines.append(f"{name}{_format_labels(labels)} {value:g}")
    for item in snapshot()["rates"]:
        lines.append(f"{item['name']}{_format_labels(tuple(sorted(item['labels'].items())))} {item['value']:g}")
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
    counts = observe_runtime_state(worker_count, allow_cached=True)
    alerts = []
    queue_depth = counts.get("queued", 0) + counts.get("retrying", 0)
    queue_limit = int(os.environ.get("MC_ALERT_QUEUE_DEPTH", "1000"))
    if queue_depth >= queue_limit:
        alerts.append(("QUEUE_DEPTH_HIGH", f"SMS queue depth is {queue_depth} (threshold {queue_limit})", {"queue_depth": queue_depth}))
    terminal = counts.get("delivered", 0) + counts.get("accepted", 0) + counts.get("failed", 0)
    failure_rate = counts.get("failed", 0) / terminal if terminal else 0
    rate_limit = float(os.environ.get("MC_ALERT_FAILURE_RATE", "0.2"))
    if terminal >= 20 and failure_rate >= rate_limit:
        alerts.append(("FAILURE_RATE_HIGH", f"SMS failure rate is {failure_rate:.1%} (threshold {rate_limit:.1%})", {"failure_rate": failure_rate}))
    if gateway_status and gateway_status.get("enabled_configs", 0) and not gateway_status.get("connected"):
        alerts.append(("ALL_SMSC_DOWN", "All enabled SMSC connections are unavailable", gateway_status))
    try:
        conn = get_connection()
        realtime_pending = conn.execute(
            """SELECT COUNT(*) count FROM ingestion_files WHERE campaign_type='realtime'
               AND status IN ('pending','retrying')"""
        ).fetchone()["count"]
        stale = conn.execute(
            """SELECT COUNT(*) count FROM ingestion_files WHERE campaign_type='realtime'
               AND status='processing' AND last_progress_at IS NOT NULL
               AND CAST(last_progress_at AS TIMESTAMP) < CURRENT_TIMESTAMP - (? * INTERVAL '1 second')""",
            (max(60, int(os.environ.get("MC_REALTIME_NO_PROGRESS_SECONDS", "900"))),),
        ).fetchone()["count"]
        conn.close()
        realtime_limit = max(1, int(os.environ.get("MC_ALERT_REALTIME_PENDING_FILES", "100")))
        if int(realtime_pending or 0) >= realtime_limit:
            alerts.append(("REALTIME_INBOX_BACKLOG_HIGH",
                           f"Realtime inbox backlog is {realtime_pending} (threshold {realtime_limit})",
                           {"pending_files": int(realtime_pending)}))
        if int(stale or 0):
            alerts.append(("REALTIME_INGESTION_STALLED",
                           f"{stale} realtime file(s) have no progress within the configured interval",
                           {"stalled_files": int(stale)}))
    except Exception:
        pass
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
