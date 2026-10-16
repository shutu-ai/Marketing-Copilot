"""One ingress contract for batch, realtime and SFTP source chunks."""

from __future__ import annotations

import hashlib
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable, Mapping

from .partition import normalize_msisdn, partition_for_msisdn
from .pressure import StageCredit
from .spool import SpoolCapacityError, SpoolRecord, TaskSpool


@dataclass
class IngressResult:
    accepted: int = 0
    duplicate: int = 0
    rejected: int = 0
    rejected_reasons: Counter = field(default_factory=Counter)
    by_partition: Counter = field(default_factory=Counter)
    by_campaign: Counter = field(default_factory=Counter)
    accepted_msisdns: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "accepted": self.accepted,
            "duplicate": self.duplicate,
            "rejected": self.rejected,
            "rejected_reasons": dict(self.rejected_reasons),
            "by_partition": dict(self.by_partition),
            "by_campaign": {f"{key[0]}:{key[1]}": value for key, value in self.by_campaign.items()},
            "accepted_msisdns": list(self.accepted_msisdns),
        }


class IngressRouter:
    """Normalize and durably route rows without opening DuckDB."""

    def __init__(self, spool: TaskSpool, *, ingress_credit: StageCredit | None = None):
        self.spool = spool
        self.ingress_credit = ingress_credit

    @staticmethod
    def event_id(*, campaign_type: str, campaign_id: int, source_id: str,
                 row_ordinal: int, campaign_generation: int) -> str:
        value = (
            f"{campaign_type}:{int(campaign_id)}:{source_id}:"
            f"{int(row_ordinal)}:{int(campaign_generation)}"
        )
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _render_message(row: Mapping, normalized: str) -> str:
        raw = row.get("message", row.get("content", ""))
        value = str(raw or "")
        if not value:
            return ""
        try:
            context = {str(key): item for key, item in row.items()}
            context["msisdn"] = normalized
            return value.format(**context)
        except (KeyError, IndexError, ValueError):
            return value

    def enqueue_chunk(self, rows: Iterable[Mapping], *, campaign_type: str,
                      campaign_id: int, campaign_generation: int = 1,
                      route_id: int = 0, phone_prefix: str = "",
                      source_id: str = "source", payload_ref: str = "",
                      row_ordinal_start: int = 0, flash_sms: bool = False,
                      source_addr: str = "") -> IngressResult:
        result = IngressResult()
        records: list[SpoolRecord] = []
        for index, row in enumerate(rows, 1):
            raw_msisdn = row.get("msisdn", row.get("phone", row.get("mobile", "")))
            normalized = normalize_msisdn(raw_msisdn, phone_prefix)
            if not normalized:
                result.rejected += 1
                result.rejected_reasons["invalid_msisdn"] += 1
                continue
            message = self._render_message(row, normalized)
            if not message:
                result.rejected += 1
                result.rejected_reasons["empty_message"] += 1
                continue
            event_id = self.event_id(
                campaign_type=campaign_type, campaign_id=campaign_id,
                source_id=source_id, row_ordinal=int(row_ordinal_start) + index,
                campaign_generation=campaign_generation,
            )
            partition_id = partition_for_msisdn(normalized, self.spool.virtual_partition_count)
            records.append(SpoolRecord(
                schema_version=1,
                event_id=event_id,
                partition_id=partition_id,
                ordinal=0,
                campaign_type=str(campaign_type),
                campaign_id=int(campaign_id),
                campaign_generation=int(campaign_generation),
                route_id=int(route_id),
                normalized_msisdn=normalized,
                payload_ref=str(payload_ref or source_id),
                message=message,
                part_count_hint=1,
                source_id=str(source_id),
                created_at=time.time(),
                flash_sms=bool(flash_sms),
                source_addr=str(row.get("source_addr", source_addr) or ""),
            ))
        if not records:
            return result
        total_bytes = sum(len(item.message.encode("utf-8")) + 256 for item in records)
        reservation = None
        if self.ingress_credit:
            reservation = self.ingress_credit.try_reserve(len(records), total_bytes)
            if not reservation:
                raise SpoolCapacityError(f"ingress stage blocked: {self.ingress_credit.snapshot()['blocked_reason']}")
        try:
            inserted, count = self.spool.append_many(records)
            result.accepted = count
            result.duplicate = len(records) - count
            for item in inserted:
                result.by_partition[item.partition_id] += 1
                result.by_campaign[(item.campaign_type, item.campaign_id)] += 1
                result.accepted_msisdns.append(item.normalized_msisdn)
        finally:
            if reservation and self.ingress_credit:
                self.ingress_credit.release(reservation)
        return result

    def enqueue_campaigns(self, campaigns: Mapping[tuple[str, int], Iterable[Mapping]], **kwargs) -> IngressResult:
        """Fairly admit one row at a time from each source campaign."""

        iterators = {key: iter(rows) for key, rows in campaigns.items()}
        result = IngressResult()
        ordinal = 0
        while iterators:
            for key in list(iterators):
                try:
                    row = next(iterators[key])
                except StopIteration:
                    iterators.pop(key, None)
                    continue
                campaign_type, campaign_id = key
                item = self.enqueue_chunk(
                    [row], campaign_type=campaign_type, campaign_id=campaign_id,
                    source_id=str(kwargs.get("source_id", f"source-{ordinal}")),
                    **{name: value for name, value in kwargs.items() if name != "source_id"},
                )
                result.accepted += item.accepted
                result.duplicate += item.duplicate
                result.rejected += item.rejected
                result.rejected_reasons.update(item.rejected_reasons)
                result.by_partition.update(item.by_partition)
                result.by_campaign.update(item.by_campaign)
                result.accepted_msisdns.extend(item.accepted_msisdns)
                ordinal += 1
        return result
