from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any, Callable

from .contract import Partition, Query


DAILY_COLUMNS = ["code", "date", "close"]
SNAPSHOT_COLUMNS = ["code", "date", "time", "close"]
FAILURE_COLUMNS = ["code", "failure"]


def _bs_code(code: str) -> str:
    symbol, exchange = code.split(".")
    return f"{exchange.lower()}.{symbol}"


def _collect(result: Any, pandas_module: Any):
    rows: list[list[str]] = []
    while result.error_code == "0" and result.next():
        rows.append(result.get_row_data())
    if result.error_code != "0":
        raise RuntimeError("market-data provider query failed")
    return pandas_module.DataFrame(rows, columns=result.fields)


def _retry(fn: Callable[[], Any], attempts: int = 3) -> Any:
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # network/provider retries only
            last = exc
            if attempt < attempts:
                time.sleep(attempt * 3)
    if last is None:
        raise RuntimeError("market-data retry exhausted without an exception")
    raise last


def _write_csv(frame: Any, path: Path) -> None:
    frame.to_csv(
        path,
        index=False,
        compression={"method": "gzip", "compresslevel": 6, "mtime": 0},
    )


def _write_checkpoint(
    root: Path,
    partition: int,
    daily_rows: list[Any],
    snapshot_rows: list[Any],
    failures: list[dict[str, str]],
    done: int,
    total: int,
) -> None:
    import pandas as pd

    daily = (
        pd.concat(daily_rows, ignore_index=True)
        if daily_rows
        else pd.DataFrame(columns=DAILY_COLUMNS)
    )
    snapshot = (
        pd.concat(snapshot_rows, ignore_index=True)
        if snapshot_rows
        else pd.DataFrame(columns=SNAPSHOT_COLUMNS)
    )
    if len(daily):
        daily = daily.drop_duplicates(["code", "date"], keep="last").sort_values(
            ["code", "date"]
        )
    if len(snapshot):
        snapshot = snapshot.drop_duplicates(["code", "date"], keep="last").sort_values(
            ["code", "date"]
        )
    _write_csv(daily[DAILY_COLUMNS], root / f"daily_partition_{partition:03d}.csv.gz")
    _write_csv(
        snapshot[SNAPSHOT_COLUMNS],
        root / f"snapshot_partition_{partition:03d}.csv.gz",
    )
    pd.DataFrame(failures, columns=FAILURE_COLUMNS).to_csv(
        root / f"failures_partition_{partition:03d}.csv", index=False
    )
    progress = {
        "schema_version": 1,
        "stage": "PUBLIC_RAW_MARKET_DATA_FETCH",
        "partition": partition,
        "symbols_done": done,
        "symbols_total": total,
        "daily_rows": int(len(daily)),
        "snapshot_rows": int(len(snapshot)),
        "failures": int(len(failures)),
        "public_data_only": True,
    }
    (root / f"progress_partition_{partition:03d}.json").write_text(
        json.dumps(progress, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )


def fetch_partition(partition: Partition, query: Query, output_root: str) -> dict[str, Any]:
    import baostock as bs
    import pandas as pd

    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    daily_rows: list[Any] = []
    snapshot_rows: list[Any] = []
    failures: list[dict[str, str]] = []

    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError("market-data provider login failed")
    try:
        for index, code in enumerate(partition.symbols, 1):
            try:
                daily = _retry(
                    lambda: _collect(
                        bs.query_history_k_data_plus(
                            _bs_code(code),
                            "date,close",
                            start_date=query.daily_start,
                            end_date=query.daily_end,
                            frequency="d",
                            adjustflag=query.adjustflag,
                        ),
                        pd,
                    )
                )
                intraday_parts = []
                for start, end in query.intraday_chunks:
                    intraday_parts.append(
                        _retry(
                            lambda start=start, end=end: _collect(
                                bs.query_history_k_data_plus(
                                    _bs_code(code),
                                    "date,time,close",
                                    start_date=start,
                                    end_date=end,
                                    frequency="5",
                                    adjustflag=query.adjustflag,
                                ),
                                pd,
                            )
                        )
                    )
                intraday = pd.concat(intraday_parts, ignore_index=True)
                if daily.empty or intraday.empty:
                    failures.append({"code": code, "failure": "empty_provider_result"})
                else:
                    daily = daily[["date", "close"]].copy()
                    daily.insert(0, "code", code)
                    intraday = intraday[["date", "time", "close"]].copy()
                    intraday["time"] = intraday["time"].astype(str)
                    intraday = intraday[
                        intraday["time"].str.slice(8, 12) == query.snapshot_hhmm
                    ]
                    if intraday.empty:
                        failures.append(
                            {"code": code, "failure": "empty_snapshot_result"}
                        )
                    else:
                        intraday.insert(0, "code", code)
                        daily_rows.append(daily[DAILY_COLUMNS])
                        snapshot_rows.append(intraday[SNAPSHOT_COLUMNS])
            except Exception as exc:
                failures.append(
                    {"code": code, "failure": f"provider:{type(exc).__name__}"}
                )

            if index % 5 == 0 or index == len(partition.symbols):
                _write_checkpoint(
                    root,
                    partition.partition,
                    daily_rows,
                    snapshot_rows,
                    failures,
                    index,
                    len(partition.symbols),
                )
            time.sleep(0.03)
    finally:
        bs.logout()

    _write_checkpoint(
        root,
        partition.partition,
        daily_rows,
        snapshot_rows,
        failures,
        len(partition.symbols),
        len(partition.symbols),
    )
    return {
        "partition": partition.partition,
        "symbols_total": len(partition.symbols),
        "failures": len(failures),
        "status": "PASS" if not failures else "FAILED_RETRYABLE",
    }
