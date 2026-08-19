from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
import io
import json
import multiprocessing
from pathlib import Path
import tarfile
import tempfile
from typing import Any

from .contract import Request
from .fetch import fetch_partition
from .sealed_protocol import canonical_json, result_aad, seal_bytes


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_archive(root: Path, manifest: dict[str, Any]) -> bytes:
    (root / "manifest.json").write_bytes(canonical_json(manifest))
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz", compresslevel=6) as archive:
        for path in sorted(root.iterdir()):
            if path.is_file():
                archive.add(path, arcname=path.name, recursive=False)
    return buffer.getvalue()


def run_group(
    request: Request,
    group: int,
    request_blob_sha256: str,
    key: bytes,
    output_path: Path,
) -> int:
    partitions = request.partitions_for_group(group)
    if not partitions:
        raise RuntimeError("validated workflow group has no partitions")
    with tempfile.TemporaryDirectory(prefix="sealed-market-data-") as temp:
        root = Path(temp)
        results: list[dict[str, Any]] = []
        fatal: list[dict[str, Any]] = []
        # Spawn fresh provider processes so the parent encryption key is not copied
        # into data-fetch worker memory by a POSIX fork.
        with ProcessPoolExecutor(
            max_workers=request.parallelism,
            mp_context=multiprocessing.get_context("spawn"),
        ) as pool:
            futures = {
                pool.submit(fetch_partition, item, request.query, str(root)): item.partition
                for item in partitions
            }
            for future in as_completed(futures):
                partition = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    fatal.append(
                        {
                            "partition": partition,
                            "status": "FAILED_RETRYABLE",
                            "failure": type(exc).__name__,
                        }
                    )

        files = [path for path in sorted(root.iterdir()) if path.is_file()]
        manifest = {
            "schema_version": 1,
            "job_id": request.job_id,
            "task": "baostock_raw_snapshot_v1",
            "public_data_only": True,
            "group": group,
            "group_count": request.group_count,
            "request_blob_sha256": request_blob_sha256,
            "partitions": sorted(results + fatal, key=lambda item: item["partition"]),
            "files": [
                {"name": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)}
                for path in files
            ],
            "status": (
                "PASS"
                if not fatal and all(item.get("status") == "PASS" for item in results)
                else "FAILED_RETRYABLE"
            ),
        }
        archive = _build_archive(root, manifest)
        sealed = seal_bytes(archive, key, result_aad(request.job_id, group))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(sealed)
        return 0 if manifest["status"] == "PASS" else 2
