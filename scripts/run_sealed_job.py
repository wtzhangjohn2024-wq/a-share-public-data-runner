from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from public_runner.contract import ContractError, JOB_ID_RE, validate_request
from public_runner.sealed_protocol import (
    ProtocolError,
    decode_key,
    decode_transport,
    request_aad,
    unseal_bytes,
)
from public_runner.worker import run_group


SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one sealed public market-data group")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--group", type=int, required=True)
    parser.add_argument("--output-dir", default="sealed-output")
    args = parser.parse_args()

    try:
        if not JOB_ID_RE.fullmatch(args.job_id):
            raise ContractError("workflow job id is invalid")
        payload_b64 = os.environ.pop("SEALED_JOB_B64", "")
        expected_digest = os.environ.pop("SEALED_JOB_SHA256", "")
        key_b64 = os.environ.pop("SEALED_JOB_KEY_B64", "")
        if not payload_b64 or len(payload_b64) > 60_000:
            raise ProtocolError("workflow payload is missing or exceeds the safety limit")
        if not SHA256_RE.fullmatch(expected_digest):
            raise ProtocolError("workflow payload digest is invalid")
        blob = decode_transport(payload_b64)
        actual_digest = hashlib.sha256(blob).hexdigest()
        if actual_digest != expected_digest:
            raise ProtocolError("workflow payload digest does not match")
        key = decode_key(key_b64)
        del key_b64
        plaintext = unseal_bytes(blob, key, request_aad(args.job_id))
        try:
            raw_request = json.loads(plaintext.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ContractError("decrypted request is invalid JSON") from exc
        request = validate_request(raw_request, args.job_id, args.group)
        output = (
            Path(args.output_dir)
            / f"sealed-result-{args.job_id}-g{args.group}.asr"
        )
        returncode = run_group(request, args.group, actual_digest, key, output)
        status = "PASS" if returncode == 0 else "FAILED_RETRYABLE"
        print(
            f"sealed public-data group {args.group} complete: status={status}; "
            f"partitions={len(request.partitions_for_group(args.group))}",
            flush=True,
        )
        return returncode
    except Exception as exc:
        print(f"sealed public-data job rejected: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
