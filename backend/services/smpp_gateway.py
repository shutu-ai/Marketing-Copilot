import logging
import threading
import time

from database import get_connection
from services import metrics_service, sms_task_service

logger = logging.getLogger(__name__)
detail_logger = logging.getLogger("mc.detail")


class SmppSubmitError(RuntimeError):
    def __init__(self, status: int, message: str = ""):
        self.status = int(status or 0)
        super().__init__(message or f"SMPP submit failed with status 0x{self.status:08x}")


class SmppDeliveryUnknownError(SmppSubmitError):
    def __init__(self, message):
        super().__init__(-1, message)

    """Submission may have reached the SMSC; retrying on a backup could duplicate it."""


class _SmppConnection:
    def __init__(self, config: dict, slot: int):
        self.config = dict(config)
        self.slot = slot
        self.client = None
        self.connected = False
        self.state = "disconnected"
        self.last_error = ""
        self.last_connected_at = None
        self._condition = threading.Condition(threading.RLock())
        self._sequence_tasks = {}
        self._outcomes = {}
        self._early_outcomes = {}
        self._listener = None
        self._last_submit = 0.0

    @staticmethod
    def _text(value) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace").strip("\x00")
        return str(value or "").strip("\x00")

    def _store_outcome(self, pdu, ok: bool):
        sequence = getattr(pdu, "sequence", None)
        outcome = {"ok": ok, "status": int(getattr(pdu, "status", 0) or 0),
                   "message_id": self._text(getattr(pdu, "message_id", ""))}
        with self._condition:
            task_id = self._sequence_tasks.get(sequence)
            target = self._outcomes if sequence in self._sequence_tasks else self._early_outcomes
            target[sequence] = outcome
            if ok and task_id and outcome["message_id"]:
                sms_task_service.append_message_id(task_id, outcome["message_id"], wait=False)
            self._condition.notify_all()
        detail_logger.info("SMPP response config=%s slot=%s sequence=%s task=%s status=%s message_id=%s",
                           self.config["id"], self.slot, sequence, task_id, outcome["status"], outcome["message_id"])

    def _on_delivery(self, pdu, **kwargs):
        import smpplib.consts
        try:
            message_id = self._text(getattr(pdu, "receipted_message_id", ""))
            state = self._text(getattr(pdu, "message_state", ""))
            raw = self._text(getattr(pdu, "short_message", ""))
            detail_logger.info(
                "SMPP delivery receipt config=%s slot=%s command=%s "
                "sequence=%s message_id=%s state=%s",
                self.config.get("id"), self.slot,
                getattr(pdu, "command", ""), getattr(pdu, "sequence", ""),
                message_id, state,
            )
            if not message_id:
                logger.warning(
                    "SMPP delivery receipt missing message_id config=%s slot=%s "
                    "sequence=%s state=%s",
                    self.config.get("id"), self.slot,
                    getattr(pdu, "sequence", ""), state,
                )
            else:
                sms_task_service.record_receipt(message_id, state, raw, wait=False)
                metrics_service.inc(
                    "marketing_copilot_dlr_total",
                    labels={"state": state or "unknown"},
                )
            return smpplib.consts.SMPP_ESME_ROK
        except Exception:
            logger.exception("Failed to handle SMPP delivery receipt")
            return smpplib.consts.SMPP_ESME_RSYSERR
    def _listen(self, client):
        try:
            client.listen(auto_send_enquire_link=True)
        except Exception as exc:
            if self.connected:
                self.last_error = str(exc)
                logger.warning("SMPP listener stopped config=%s slot=%s: %s", self.config["id"], self.slot, exc)
        finally:
            self.connected = False
            self.state = "disconnected"
            metrics_service.set_gauge("marketing_copilot_smpp_connection", 0,
                                      {"config_id": self.config["id"], "slot": self.slot})

    def connect(self):
        with self._condition:
            if self.connected and self._listener and self._listener.is_alive():
                return
            self.disconnect()
            self.state = "connecting"
            try:
                import smpplib.client
                client = smpplib.client.Client(
                    self.config["host"], int(self.config["port"]), allow_unknown_opt_params=True,
                    timeout=int(self.config.get("timeout") or 10))
                client.set_message_sent_handler(lambda pdu, **kw: self._store_outcome(pdu, True))
                client.set_message_received_handler(self._on_delivery)
                client.set_error_pdu_handler(lambda pdu, **kw: self._store_outcome(pdu, False))
                client.connect()
                bind_type = self.config.get("bind_type") or "transceiver"
                getattr(client, f"bind_{bind_type}", client.bind_transceiver)(
                    system_id=self.config["system_id"], password=self.config["password"])
                self.client, self.connected = client, True
                self.state, self.last_error, self.last_connected_at = "bound", "", time.time()
                self._listener = threading.Thread(target=self._listen, args=(client,),
                                                  name=f"smpp-{self.config['id']}-{self.slot}", daemon=True)
                self._listener.start()
                metrics_service.inc("marketing_copilot_smpp_connect_total", labels={"config_id": self.config["id"]})
                metrics_service.set_gauge("marketing_copilot_smpp_connection", 1,
                                          {"config_id": self.config["id"], "slot": self.slot})
            except Exception as exc:
                self.connected, self.client, self.state, self.last_error = False, None, "error", str(exc)
                metrics_service.inc("marketing_copilot_smpp_connect_failures_total", labels={"config_id": self.config["id"]})
                raise

    def disconnect(self):
        client, self.client = self.client, None
        self.connected, self.state = False, "disconnected"
        if client:
            try: client.unbind()
            except Exception: pass
            try: client.disconnect()
            except Exception: pass
        listener = self._listener
        if listener and listener is not threading.current_thread():
            listener.join(timeout=2)
        self._listener = None
        metrics_service.set_gauge("marketing_copilot_smpp_connection", 0,
                                  {"config_id": self.config.get("id", 0), "slot": self.slot})

    def _throttle(self):
        interval = 1.0 / max(1, int(self.config.get("tps") or 1))
        wait = interval - (time.monotonic() - self._last_submit)
        if wait > 0:
            time.sleep(wait)
        self._last_submit = time.monotonic()

    def send(self, task: dict) -> list[str]:
        with self._condition:
            self.connect()
            import smpplib.consts
            import smpplib.gsm
            parts, coding, esm_class = smpplib.gsm.make_parts(task["content"])
            source_addr = task.get("source_addr") or self.config.get("source_addr") or "Marketing"
            content = task.get("content") or ""
            detail_logger.info(
                "[SMPP SEND] task=%s campaign=%s/%s name=%s msisdn=%s source_addr=%s "
                "config=%s %s:%s slot=%s parts=%d content_len=%d content=%s",
                task.get("id"), task.get("campaign_type"), task.get("campaign_id"),
                task.get("campaign_name", ""), task.get("msisdn"), source_addr,
                self.config.get("id"), self.config.get("host"), self.config.get("port"), self.slot,
                len(parts), len(content), content[:1000],
            )
            sequences = []
            submitted_any = False
            try:
                for part in parts:
                    self._throttle()
                    pdu = self.client.send_message(
                       source_addr_ton=int(self.config.get("address_ton") or smpplib.consts.SMPP_TON_ALNUM),
                       source_addr_npi=int(self.config.get("address_npi") or smpplib.consts.SMPP_NPI_UNK),
                       source_addr=source_addr, dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
                        dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN, destination_addr=task["msisdn"],
                        short_message=part, data_coding=coding, esm_class=esm_class,
                        registered_delivery=(self.config.get('delivery_receipt_mode') or 'optional') != 'disabled')
                    submitted_any = True
                    sequence = pdu.sequence
                    self._sequence_tasks[sequence] = task["id"]
                    if sequence in self._early_outcomes:
                        outcome = self._early_outcomes.pop(sequence)
                        self._outcomes[sequence] = outcome
                        if outcome["ok"] and outcome["message_id"]:
                            sms_task_service.append_message_id(task["id"], outcome["message_id"], wait=False)
                    sequences.append(sequence)
                deadline = time.monotonic() + int(self.config.get("timeout") or 10)
                while any(seq not in self._outcomes for seq in sequences):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise SmppDeliveryUnknownError("submit_sm_resp timeout; delivery status unknown")
                    self._condition.wait(min(remaining, 0.25))
                outcomes = [self._outcomes.pop(seq) for seq in sequences]
                failed = next((item for item in outcomes if not item["ok"]), None)
                if failed:
                    raise SmppSubmitError(failed["status"])
                detail_logger.info(
                    "[SMPP SEND OK] task=%s sequences=%s message_ids=%s",
                    task.get("id"), sequences,
                    [item.get("message_id") for item in outcomes if item.get("message_id")],
                )
                metrics_service.inc("marketing_copilot_smpp_submitted_total",
                                    labels={"config_id": self.config["id"]})
                return [item["message_id"] for item in outcomes if item["message_id"]]
            except (SmppSubmitError, SmppDeliveryUnknownError):
                raise
            except Exception as exc:
                if submitted_any:
                    raise SmppDeliveryUnknownError(f"connection lost after submit: {exc}") from exc
                raise
            finally:
                for seq in sequences:
                    self._sequence_tasks.pop(seq, None)

    def status(self):
        return {"slot": self.slot, "state": self.state, "connected": self.connected,
                "last_error": self.last_error, "last_connected_at": self.last_connected_at}


class SmppGateway:
    def __init__(self, connection_factory=_SmppConnection):
        self._connection_factory = connection_factory
        self._lock = threading.RLock()
        self._pools = {}
        self._round_robin = {}
        self._down_until = {}
        self._last_error = ""
        self._active_config_id = None
        self._config_cache = None
        self._config_cache_version = 0

    def invalidate_config_cache(self):
        with self._lock:
            self._config_cache = None
            self._config_cache_version += 1

    def _enabled_configs(self):
        with self._lock:
            if self._config_cache is not None:
                return [dict(config) for config in self._config_cache]
            generation = self._config_cache_version
        conn = get_connection()
        rows = conn.execute("SELECT * FROM smpp_configs WHERE enabled=1 ORDER BY priority,id").fetchall()
        conn.close()
        configs = [dict(row) for row in rows]
        with self._lock:
            if generation == self._config_cache_version:
                self._config_cache = configs
        return [dict(config) for config in configs]

    @property
    def connected(self):
        return any(connection.connected for pool in self._pools.values() for connection in pool)

    def _connection_for(self, config: dict):
        config_id = config["id"]
        with self._lock:
            pool = self._pools.setdefault(config_id, [])
            desired = max(1, min(20, int(config.get("pool_size") or 1)))
            pool[:] = [item for item in pool if item.config.get("updated_at") == config.get("updated_at")]
            if len(pool) < desired:
                connection = self._connection_factory(config, len(pool) + 1)
                pool.append(connection)
            else:
                index = self._round_robin.get(config_id, 0) % len(pool)
                connection = pool[index]
                self._round_robin[config_id] = index + 1
        connection.connect()
        return connection

    def connect(self):
        configs = self._enabled_configs()
        if not configs:
            raise RuntimeError("No enabled SMPP configuration")
        errors = []
        for config in configs:
            if time.monotonic() < self._down_until.get(config["id"], 0):
                continue
            try:
                desired = max(1, min(20, int(config.get("pool_size") or 1)))
                for _ in range(desired):
                    self._connection_for(config)
                self._active_config_id = config["id"]
                return
            except Exception as exc:
                errors.append(f"{config['name']}: {exc}")
                self._mark_down(config, exc)
        raise RuntimeError("All SMSC configurations unavailable: " + "; ".join(errors))

    def _mark_down(self, config: dict, exc: Exception):
        delay = max(1, min(300, int(config.get("reconnect_max_seconds") or 60)))
        self._down_until[config["id"]] = time.monotonic() + delay
        self._last_error = str(exc)
        metrics_service.inc("marketing_copilot_smpp_failures_total", labels={"config_id": config["id"]})
        for connection in self._pools.get(config["id"], []):
            if not connection.connected:
                connection.disconnect()

    def send(self, task: dict) -> list[str]:
        configs = self._enabled_configs()
        if not configs:
            raise RuntimeError("No enabled SMPP configuration")
        target_id = task.get("smpp_config_id")
        if target_id:
            config = next((c for c in configs if c["id"] == target_id), None)
            if not config:
                raise RuntimeError(
                    f"Associated SMPP configuration {target_id} is missing or disabled")
            connection = self._connection_for(config)
            task['_delivery_receipt_mode'] = config.get('delivery_receipt_mode') or 'optional'
            self._active_config_id = config['id']
            return connection.send(task)
        previous_id = None
        errors = []
        for config in configs:
            if time.monotonic() < self._down_until.get(config["id"], 0):
                continue
            try:
                connection = self._connection_for(config)
                if previous_id is not None and previous_id != config["id"]:
                    metrics_service.inc("marketing_copilot_smpp_failovers_total",
                                        labels={"from_config": previous_id, "to_config": config["id"]})
                task['_delivery_receipt_mode'] = config.get('delivery_receipt_mode') or 'optional'
                result = connection.send(task)
                self._active_config_id = config["id"]
                return result
            except (SmppSubmitError, SmppDeliveryUnknownError):
                raise
            except Exception as exc:
                previous_id = config["id"]
                errors.append(f"{config['name']}: {exc}")
                self._mark_down(config, exc)
                logger.warning("SMSC unavailable, trying backup config=%s error=%s", config["id"], exc)
        raise RuntimeError("All SMSC configurations unavailable: " + "; ".join(errors))

    def disconnect(self, config_id=None):
        with self._lock:
            targets = [config_id] if config_id is not None else list(self._pools)
            for target in targets:
                for connection in self._pools.pop(target, []):
                    connection.disconnect()
            if config_id is None:
                self._active_config_id = None

    def recover(self, config_id=None):
        """Immediately rebuild the live pool after an operator confirms recovery."""
        configs = self._enabled_configs()
        targets = [item for item in configs if config_id is None or item["id"] == config_id]
        if config_id is not None and not targets:
            raise RuntimeError(f"SMPP configuration {config_id} is missing or disabled")
        for config in targets:
            self._down_until.pop(config["id"], None)
            self.disconnect(config["id"])
            desired = max(1, min(20, int(config.get("pool_size") or 1)))
            for _ in range(desired):
                self._connection_for(config)
            self._active_config_id = config["id"]
        self._last_error = ""
        return self.status()

    def status(self) -> dict:
        configs = self._enabled_configs()
        pools = []
        for config in configs:
            connections = [item.status() for item in self._pools.get(config["id"], [])]
            pools.append({"config_id": config["id"], "name": config["name"], "priority": config["priority"],
                          "desired_pool_size": config["pool_size"], "connections": connections,
                          "healthy": any(item["connected"] for item in connections),
                          "cooldown_seconds": max(0, round(self._down_until.get(config["id"], 0) - time.monotonic(), 1))})
        return {"state": "bound" if self.connected else "disconnected", "connected": self.connected,
                "config_id": self._active_config_id, "active_config_id": self._active_config_id,
                "enabled_configs": len(configs), "pools": pools, "last_error": self._last_error}


gateway = SmppGateway()
