from __future__ import annotations

import base64
import hashlib
import json
import os
import struct

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


MAGIC = b"ASR1"
MAX_HEADER_BYTES = 16 * 1024


class ProtocolError(RuntimeError):
    """Raised when a sealed payload is malformed or fails authentication."""


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def decode_key(value: str) -> bytes:
    try:
        key = base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as exc:
        raise ProtocolError("runner key is not valid base64") from exc
    if len(key) != 32:
        raise ProtocolError("runner key must decode to exactly 32 bytes")
    return key


def key_id(key: bytes) -> str:
    if len(key) != 32:
        raise ProtocolError("runner key must be exactly 32 bytes")
    return hashlib.sha256(key).hexdigest()[:16]


def request_aad(job_id: str) -> bytes:
    return f"a-share-public-data-runner:v1:request:{job_id}".encode("ascii")


def result_aad(job_id: str, group: int) -> bytes:
    return f"a-share-public-data-runner:v1:result:{job_id}:{group}".encode("ascii")


def seal_bytes(plaintext: bytes, key: bytes, aad: bytes) -> bytes:
    if len(key) != 32:
        raise ProtocolError("runner key must be exactly 32 bytes")
    nonce = os.urandom(12)
    header = {
        "schema_version": 1,
        "algorithm": "AES-256-GCM",
        "key_id": key_id(key),
        "nonce_b64": base64.b64encode(nonce).decode("ascii"),
        "aad_sha256": hashlib.sha256(aad).hexdigest(),
        "plaintext_sha256": hashlib.sha256(plaintext).hexdigest(),
    }
    encoded_header = canonical_json(header)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, aad)
    return MAGIC + struct.pack(">I", len(encoded_header)) + encoded_header + ciphertext


def unseal_bytes(blob: bytes, key: bytes, aad: bytes) -> bytes:
    if len(blob) < len(MAGIC) + 4 + 16 or not blob.startswith(MAGIC):
        raise ProtocolError("sealed payload has an invalid header")
    header_size = struct.unpack(">I", blob[len(MAGIC) : len(MAGIC) + 4])[0]
    if not 0 < header_size <= MAX_HEADER_BYTES:
        raise ProtocolError("sealed payload header length is invalid")
    header_start = len(MAGIC) + 4
    header_end = header_start + header_size
    if header_end + 16 > len(blob):
        raise ProtocolError("sealed payload is truncated")
    try:
        header = json.loads(blob[header_start:header_end].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("sealed payload header is invalid JSON") from exc
    if not isinstance(header, dict):
        raise ProtocolError("sealed payload header must be an object")
    if header.get("schema_version") != 1 or header.get("algorithm") != "AES-256-GCM":
        raise ProtocolError("sealed payload protocol is unsupported")
    if header.get("key_id") != key_id(key):
        raise ProtocolError("sealed payload key id does not match")
    if header.get("aad_sha256") != hashlib.sha256(aad).hexdigest():
        raise ProtocolError("sealed payload context does not match")
    try:
        nonce = base64.b64decode(str(header.get("nonce_b64", "")), validate=True)
    except ValueError as exc:
        raise ProtocolError("sealed payload nonce is invalid") from exc
    if len(nonce) != 12:
        raise ProtocolError("sealed payload nonce length is invalid")
    try:
        plaintext = AESGCM(key).decrypt(nonce, blob[header_end:], aad)
    except InvalidTag as exc:
        raise ProtocolError("sealed payload authentication failed") from exc
    if header.get("plaintext_sha256") != hashlib.sha256(plaintext).hexdigest():
        raise ProtocolError("sealed payload plaintext digest does not match")
    return plaintext


def encode_transport(blob: bytes) -> str:
    return base64.b64encode(blob).decode("ascii")


def decode_transport(value: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as exc:
        raise ProtocolError("workflow payload is not valid base64") from exc

