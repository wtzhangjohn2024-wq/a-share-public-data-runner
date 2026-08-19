from __future__ import annotations

import copy
import unittest

from public_runner.contract import ContractError, validate_request
from public_runner.sealed_protocol import (
    ProtocolError,
    canonical_json,
    request_aad,
    seal_bytes,
    unseal_bytes,
)


JOB_ID = "1" * 32


def sample_request() -> dict:
    return {
        "schema_version": 1,
        "job_id": JOB_ID,
        "task": "baostock_raw_snapshot_v1",
        "public_data_only": True,
        "group_count": 6,
        "parallelism": 2,
        "partitions": [
            {"partition": 0, "symbols": ["600000.SH"]},
            {"partition": 1, "symbols": ["000001.SZ"]},
        ],
        "query": {
            "daily_start": "2019-08-01",
            "daily_end": "2025-12-31",
            "intraday_chunks": [
                ["2020-01-01", "2022-12-31"],
                ["2023-01-01", "2025-12-31"],
            ],
            "snapshot_hhmm": "1445",
            "adjustflag": "3",
        },
    }


class SealedProtocolTests(unittest.TestCase):
    def test_round_trip_and_context_binding(self) -> None:
        key = bytes(range(32))
        plaintext = canonical_json(sample_request())
        blob = seal_bytes(plaintext, key, request_aad(JOB_ID))
        self.assertEqual(unseal_bytes(blob, key, request_aad(JOB_ID)), plaintext)
        with self.assertRaises(ProtocolError):
            unseal_bytes(blob, key, request_aad("2" * 32))

    def test_tamper_is_rejected(self) -> None:
        key = bytes(range(32))
        blob = bytearray(seal_bytes(b"private", key, request_aad(JOB_ID)))
        blob[-1] ^= 1
        with self.assertRaises(ProtocolError):
            unseal_bytes(bytes(blob), key, request_aad(JOB_ID))


class PublicDataContractTests(unittest.TestCase):
    def test_valid_request_is_partitioned_by_group(self) -> None:
        request = validate_request(sample_request(), JOB_ID, 1)
        self.assertEqual(
            [item.partition for item in request.partitions_for_group(1)], [1]
        )

    def test_strategy_field_is_rejected(self) -> None:
        value = sample_request()
        value["strategy_score"] = 1
        with self.assertRaises(ContractError):
            validate_request(value, JOB_ID, 0)

    def test_duplicate_symbol_is_rejected(self) -> None:
        value = sample_request()
        value["partitions"][1]["symbols"] = ["600000.SH"]
        with self.assertRaises(ContractError):
            validate_request(value, JOB_ID, 0)

    def test_invalid_symbol_is_rejected(self) -> None:
        value = copy.deepcopy(sample_request())
        value["partitions"][0]["symbols"] = ["BTC-USD"]
        with self.assertRaises(ContractError):
            validate_request(value, JOB_ID, 0)


if __name__ == "__main__":
    unittest.main()
