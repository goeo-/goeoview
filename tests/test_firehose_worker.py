"""Tests for firehose_worker module.

Tests the validate_commit() function which performs CPU-bound commit
validation in ProcessPoolExecutor workers.
"""

import cbrrr

from goeoview.firehose_worker import validate_commit


class TestValidateCommit:
    """Test validate_commit return values for special cases."""

    def test_worker_returns_skip_for_toobig(self):
        """A tooBig message should return ok=True with empty rows."""
        message = cbrrr.encode_dag_cbor({
            "repo": "did:plc:fake",
            "rev": "aaaaaaa",
            "since": None,
            "time": "2025-01-01T00:00:00Z",
            "seq": 1,
            "tooBig": True,
            "blocks": b"",
            "commit": None,
            "ops": [],
        })

        result = validate_commit(message, "zFAKEKEY")

        assert result["ok"] is True
        assert result["rows"] == []
        assert result["repo"] is None

    def test_worker_returns_error_for_bad_car(self):
        """Invalid blocks data should return ok=False with an error string."""
        message = cbrrr.encode_dag_cbor({
            "repo": "did:plc:fake",
            "rev": "aaaaaaa",
            "since": None,
            "time": "2025-01-01T00:00:00Z",
            "seq": 42,
            "tooBig": False,
            "blocks": b"this is not a valid CAR file",
            "commit": None,
            "ops": [],
        })

        result = validate_commit(message, "zFAKEKEY")

        assert result["ok"] is False
        assert "error" in result
        assert result["repo"] == "did:plc:fake"
        assert result["seq"] == 42

    def test_worker_returns_retry_on_no_key(self):
        """When atproto_key is None, should return retry sentinel."""
        message = cbrrr.encode_dag_cbor({
            "repo": "did:plc:fake",
            "rev": "aaaaaaa",
            "since": None,
            "time": "2025-01-01T00:00:00Z",
            "seq": 99,
            "tooBig": False,
            "blocks": b"",
            "commit": None,
            "ops": [],
        })

        result = validate_commit(message, None)

        assert result["ok"] is False
        assert result["retry"] is True
        assert result["repo"] == "did:plc:fake"
        assert result["seq"] == 99
