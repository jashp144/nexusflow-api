"""Tests for HMAC-SHA256 signature generation and timestamp replay protection."""

import json
import time

from nexusflow.dispatch.signer import WebhookSigner


def test_signature_deterministic_generation():
    """Verify HMAC-SHA256 signature is deterministic for canonical payloads."""
    secret = "test-secret-key-1234"
    timestamp = 1700000000
    payload = {"action": "order.completed", "amount": 99.99}

    sig1 = WebhookSigner.sign(secret, timestamp, payload)
    sig2 = WebhookSigner.sign(secret, timestamp, payload)

    assert isinstance(sig1, str)
    assert len(sig1) == 64  # SHA-256 hex string length
    assert sig1 == sig2


def test_signature_different_payload_types():
    """Verify string, bytes, and dictionary payloads are handled properly."""
    secret = "secret"
    ts = 1700000000

    dict_payload = {"key": "val"}
    canon_str = json.dumps(dict_payload, separators=(",", ":"), sort_keys=True)
    bytes_payload = canon_str.encode("utf-8")

    sig_dict = WebhookSigner.sign(secret, ts, dict_payload)
    sig_str = WebhookSigner.sign(secret, ts, canon_str)
    sig_bytes = WebhookSigner.sign(secret, ts, bytes_payload)

    assert sig_dict == sig_str
    assert sig_dict == sig_bytes


def test_signature_verification_success():
    """Verify correct signature and fresh timestamp pass verification."""
    secret = "secure_token"
    now = time.time()
    payload = {"event": "user.signup", "id": 42}

    sig = WebhookSigner.sign(secret, now, payload)
    is_valid = WebhookSigner.verify(secret, now, payload, sig, tolerance_seconds=300.0)
    assert is_valid is True

    # Test v1= prefix format
    is_valid_prefix = WebhookSigner.verify(
        secret, now, payload, f"v1={sig}", tolerance_seconds=300.0
    )
    assert is_valid_prefix is True


def test_signature_verification_tampered_payload():
    """Verify verification fails when payload is altered."""
    secret = "secure_token"
    now = time.time()
    payload = {"event": "payment.processed", "amount": 100}

    sig = WebhookSigner.sign(secret, now, payload)

    tampered = {"event": "payment.processed", "amount": 9999}
    assert WebhookSigner.verify(secret, now, tampered, sig) is False


def test_signature_verification_wrong_secret():
    """Verify verification fails with incorrect secret."""
    now = time.time()
    payload = {"event": "ping"}

    sig = WebhookSigner.sign("secret_a", now, payload)
    assert WebhookSigner.verify("secret_b", now, payload, sig) is False


def test_timestamp_replay_attack_protection():
    """Verify expired timestamps outside tolerance window are rejected."""
    secret = "secure_token"
    current_time = 10000.0
    tolerance = 60.0  # 1 minute tolerance

    payload = {"action": "fund.transfer"}

    # Timestamp 10 seconds ago: valid
    recent_ts = current_time - 10.0
    sig_recent = WebhookSigner.sign(secret, recent_ts, payload)
    assert (
        WebhookSigner.verify(
            secret, recent_ts, payload, sig_recent, tolerance, current_time=current_time
        )
        is True
    )

    # Timestamp 500 seconds ago: replay attack rejected
    old_ts = current_time - 500.0
    sig_old = WebhookSigner.sign(secret, old_ts, payload)
    assert (
        WebhookSigner.verify(secret, old_ts, payload, sig_old, tolerance, current_time=current_time)
        is False
    )

    # Timestamp 500 seconds in the future: clock skew / future replay rejected
    future_ts = current_time + 500.0
    sig_future = WebhookSigner.sign(secret, future_ts, payload)
    assert (
        WebhookSigner.verify(
            secret, future_ts, payload, sig_future, tolerance, current_time=current_time
        )
        is False
    )
