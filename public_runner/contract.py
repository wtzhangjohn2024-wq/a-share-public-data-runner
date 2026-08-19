from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
from typing import Any


JOB_ID_RE = re.compile(r"^[a-f0-9]{32}$")
SYMBOL_RE = re.compile(r"^[0-9]{6}\.(?:SH|SZ)$")
HHMM_RE = re.compile(r"^[0-2][0-9][0-5][0-9]$")
ROOT_KEYS = {
    "schema_version",
    "job_id",
    "task",
    "public_data_only",
    "group_count",
    "parallelism",
    "partitions",
    "query",
}
QUERY_KEYS = {
    "daily_start",
    "daily_end",
    "intraday_chunks",
    "snapshot_hhmm",
    "adjustflag",
}
PARTITION_KEYS = {"partition", "symbols"}
FORBIDDEN_KEY_TOKENS = {
    "account",
    "candidate",
    "gate",
    "holding",
    "order",
    "parameter",
    "pnl",
    "position",
    "rank",
    "ranking",
    "regime",
    "return",
    "score",
    "signal",
    "strategy",
    "threshold",
}


class ContractError(RuntimeError):
    """Raised when a request exceeds the public-data-only boundary."""


@dataclass(frozen=True)
class Query:
    daily_start: str
    daily_end: str
    intraday_chunks: tuple[tuple[str, str], ...]
    snapshot_hhmm: str
    adjustflag: str


@dataclass(frozen=True)
class Partition:
    partition: int
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class Request:
    job_id: str
    group_count: int
    parallelism: int
    partitions: tuple[Partition, ...]
    query: Query

    def partitions_for_group(self, group: int) -> tuple[Partition, ...]:
        return tuple(
            item for item in self.partitions if item.partition % self.group_count == group
        )


def _parse_date(value: Any, label: str) -> date:
    if not isinstance(value, str):
        raise ContractError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ContractError(f"{label} must be an ISO date") from exc
    if not date(2000, 1, 1) <= parsed <= date(2035, 12, 31):
        raise ContractError(f"{label} is outside the allowed market-data range")
    return parsed


def _reject_forbidden_keys(value: Any) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            tokens = set(str(key).lower().replace("-", "_").split("_"))
            if tokens & FORBIDDEN_KEY_TOKENS:
                raise ContractError("request contains a non-market-data field")
            _reject_forbidden_keys(child)
    elif isinstance(value, list):
        for child in value:
            _reject_forbidden_keys(child)


def validate_request(value: Any, expected_job_id: str, expected_group: int) -> Request:
    if not isinstance(value, dict):
        raise ContractError("request root must be an object")
    _reject_forbidden_keys(value)
    if set(value) != ROOT_KEYS:
        raise ContractError("request fields do not match the public-data contract")
    if value.get("schema_version") != 1:
        raise ContractError("request schema version is unsupported")
    if value.get("task") != "baostock_raw_snapshot_v1":
        raise ContractError("request task is unsupported")
    if value.get("public_data_only") is not True:
        raise ContractError("request must declare the public-data-only boundary")
    job_id = value.get("job_id")
    if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id):
        raise ContractError("job id is invalid")
    if job_id != expected_job_id:
        raise ContractError("job id does not match the workflow input")

    group_count = value.get("group_count")
    parallelism = value.get("parallelism")
    if group_count != 6:
        raise ContractError("group_count must match the fixed six-job workflow")
    if not isinstance(expected_group, int) or not 0 <= expected_group < group_count:
        raise ContractError("workflow group is outside the request range")
    if parallelism != 2:
        raise ContractError("parallelism must match the fixed two-process limit")

    raw_query = value.get("query")
    if not isinstance(raw_query, dict) or set(raw_query) != QUERY_KEYS:
        raise ContractError("query fields do not match the market-data contract")
    daily_start = _parse_date(raw_query.get("daily_start"), "daily_start")
    daily_end = _parse_date(raw_query.get("daily_end"), "daily_end")
    if daily_start > daily_end or (daily_end - daily_start).days > 4000:
        raise ContractError("daily query window is invalid")
    chunks: list[tuple[str, str]] = []
    raw_chunks = raw_query.get("intraday_chunks")
    if not isinstance(raw_chunks, list) or not 1 <= len(raw_chunks) <= 8:
        raise ContractError("intraday_chunks must contain between 1 and 8 windows")
    for index, raw_chunk in enumerate(raw_chunks):
        if not isinstance(raw_chunk, list) or len(raw_chunk) != 2:
            raise ContractError("each intraday chunk must be a two-date list")
        start = _parse_date(raw_chunk[0], f"intraday_chunks[{index}].start")
        end = _parse_date(raw_chunk[1], f"intraday_chunks[{index}].end")
        if start > end or (end - start).days > 1500:
            raise ContractError("intraday query chunk is invalid")
        chunks.append((start.isoformat(), end.isoformat()))
    hhmm = raw_query.get("snapshot_hhmm")
    if not isinstance(hhmm, str) or not HHMM_RE.fullmatch(hhmm):
        raise ContractError("snapshot_hhmm is invalid")
    if not "0930" <= hhmm <= "1500":
        raise ContractError("snapshot_hhmm is outside A-share trading hours")
    adjustflag = raw_query.get("adjustflag")
    if adjustflag not in {"1", "2", "3"}:
        raise ContractError("adjustflag is unsupported")

    raw_partitions = value.get("partitions")
    if not isinstance(raw_partitions, list) or not 1 <= len(raw_partitions) <= 128:
        raise ContractError("partitions must contain between 1 and 128 items")
    partitions: list[Partition] = []
    seen_partitions: set[int] = set()
    seen_symbols: set[str] = set()
    for raw_partition in raw_partitions:
        if not isinstance(raw_partition, dict) or set(raw_partition) != PARTITION_KEYS:
            raise ContractError("partition fields do not match the contract")
        partition = raw_partition.get("partition")
        symbols = raw_partition.get("symbols")
        if not isinstance(partition, int) or not 0 <= partition <= 255:
            raise ContractError("partition id must be between 0 and 255")
        if partition in seen_partitions:
            raise ContractError("partition ids must be unique")
        if not isinstance(symbols, list) or not symbols:
            raise ContractError("each partition must contain symbols")
        normalized: list[str] = []
        for symbol in symbols:
            if not isinstance(symbol, str) or not SYMBOL_RE.fullmatch(symbol):
                raise ContractError("request contains an invalid SH/SZ symbol")
            if symbol in seen_symbols:
                raise ContractError("symbols must not appear in multiple partitions")
            seen_symbols.add(symbol)
            normalized.append(symbol)
        seen_partitions.add(partition)
        partitions.append(Partition(partition=partition, symbols=tuple(sorted(normalized))))
    if len(seen_symbols) > 2000:
        raise ContractError("request exceeds the 2,000-symbol safety limit")
    if not any(item.partition % group_count == expected_group for item in partitions):
        raise ContractError("workflow group has no assigned partition")

    return Request(
        job_id=job_id,
        group_count=group_count,
        parallelism=parallelism,
        partitions=tuple(sorted(partitions, key=lambda item: item.partition)),
        query=Query(
            daily_start=daily_start.isoformat(),
            daily_end=daily_end.isoformat(),
            intraday_chunks=tuple(chunks),
            snapshot_hhmm=hhmm,
            adjustflag=adjustflag,
        ),
    )
