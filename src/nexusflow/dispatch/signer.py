"""Cryptographic HMAC-SHA256 webhook signature generation and replay protection."""

import hashlib
import hmac
import json
import time
from typing import Any


class WebhookSigner:
    """Provides HMAC-SHA256 signature generation and timestamp-based replay attack validation.

    Ensures downstream webhook recipients can authenticate that incoming payloads
    originated from nexusflow and have not been altered or replayed.
    """

    @staticmethod
    def canonicalize_payload(payload: str | bytes | dict[str, Any]) -> str:
        """Convert payload to canonical string representation."""
        if isinstance(payload, bytes):
            return payload.decode("utf-8")
        if isinstance(payload, dict):
            return json.dumps(payload, separators=(",", ":"), sort_keys=True)
        return str(payload)

    @classmethod
    def sign(
        cls,
        secret: str,
        timestamp: int | float,
        payload: str | bytes | dict[str, Any],
    ) -> str:
        """Compute HMAC-SHA256 signature over timestamp and canonical payload.

        Signature format: HMAC_SHA256(secret, "{timestamp}.{canonical_payload}")

        Args:
            secret: Shared secret string between sender and receiver.
            timestamp: Epoch timestamp (in seconds).
            payload: Payload dict, JSON string, or raw bytes.

        Returns:
            Hex-encoded HMAC-SHA256 signature string.
        """
        ts_int = int(timestamp)
        canon_payload = cls.canonicalize_payload(payload)
        signed_payload = f"{ts_int}.{canon_payload}".encode()
        return hmac.new(
            secret.encode("utf-8"),
            signed_payload,
            hashlib.sha256,
        ).hexdigest()

    @classmethod
    def verify(
        cls,
        secret: str,
        timestamp: int | float,
        payload: str | bytes | dict[str, Any],
        signature: str,
        tolerance_seconds: float = 300.0,
        current_time: int | float | None = None,
    ) -> bool:
        """Verify HMAC-SHA256 signature and protect against replay attacks.

        Args:
            secret: Shared secret string.
            timestamp: Epoch timestamp provided in webhook header.
            payload: Payload received.
            signature: Hex signature received in header (e.g. `X-Nexus-Signature`).
            tolerance_seconds: Maximum allowed clock drift / age window in seconds (default 5 min).
            current_time: Optional explicit reference timestamp for deterministic testing.

        Returns:
            True if signature matches and timestamp is within acceptable tolerance; False otherwise.
        """
        now = time.time() if current_time is None else float(current_time)
        ts_float = float(timestamp)

        # Check timestamp replay window
        if abs(now - ts_float) > tolerance_seconds:
            return False

        # Support signature strings that may have a version prefix (e.g., 'v1=...') or raw hex
        clean_sig = signature.strip()
        if clean_sig.startswith("v1="):
            clean_sig = clean_sig[3:]

        expected_sig = cls.sign(secret, ts_float, payload)
        return hmac.compare_digest(expected_sig, clean_sig)
