"""Tests for PLC helper functions.

Tests for goeoview/plc.py focusing on:
- extract_rotation_keys() - Extract rotation keys from PLC operations
- extract_handle() - Extract handle from PLC operations
- extract_services() - Extract service endpoints
- extract_verification_keys() - Extract verification keys
"""

from goeoview.plc import (
    extract_rotation_keys,
    extract_handle,
    extract_services,
    extract_verification_keys,
)


# --- extract_rotation_keys ---


def test_extract_rotation_keys_with_rotation_keys():
    operation = {
        "rotationKeys": ["did:key:key1", "did:key:key2", "did:key:key3"]
    }
    assert extract_rotation_keys(operation) == ["did:key:key1", "did:key:key2", "did:key:key3"]


def test_extract_rotation_keys_with_recovery_key():
    operation = {
        "recoveryKey": "did:key:recovery",
        "signingKey": "did:key:signing",
    }
    assert extract_rotation_keys(operation) == ["did:key:recovery", "did:key:signing"]


def test_extract_rotation_keys_empty_operation():
    assert extract_rotation_keys({}) == []


def test_extract_rotation_keys_prefers_rotation_keys():
    operation = {
        "rotationKeys": ["did:key:rotation"],
        "recoveryKey": "did:key:recovery",
        "signingKey": "did:key:signing",
    }
    assert extract_rotation_keys(operation) == ["did:key:rotation"]


def test_extract_rotation_keys_single_key():
    operation = {"rotationKeys": ["did:key:single"]}
    assert extract_rotation_keys(operation) == ["did:key:single"]


# --- extract_handle ---


def test_extract_handle_from_also_known_as():
    operation = {"alsoKnownAs": ["at://test.bsky.social"]}
    assert extract_handle(operation) == "test.bsky.social"


def test_extract_handle_from_also_known_as_multiple():
    operation = {
        "alsoKnownAs": [
            "https://example.com",
            "at://myhandle.bsky.social",
            "at://other.handle",
        ]
    }
    assert extract_handle(operation) == "myhandle.bsky.social"


def test_extract_handle_from_handle_field():
    operation = {"handle": "direct.handle.com"}
    assert extract_handle(operation) == "direct.handle.com"


def test_extract_handle_prefers_also_known_as():
    operation = {
        "alsoKnownAs": ["at://preferred.handle"],
        "handle": "fallback.handle",
    }
    assert extract_handle(operation) == "preferred.handle"


def test_extract_handle_returns_none_when_missing():
    assert extract_handle({}) is None


def test_extract_handle_also_known_as_no_at_protocol():
    """alsoKnownAs present but no at:// entries; handle field is not consulted."""
    operation = {
        "alsoKnownAs": ["https://example.com"],
        "handle": "fallback.handle",
    }
    assert extract_handle(operation) is None


def test_extract_handle_empty_also_known_as():
    operation = {"alsoKnownAs": []}
    assert extract_handle(operation) is None


# --- extract_services ---


def test_extract_services_all_services():
    operation = {
        "services": {
            "atproto_pds": {"endpoint": "https://pds.example.com"},
            "atproto_labeler": {"endpoint": "https://labeler.example.com"},
            "bsky_chat": {"endpoint": "https://chat.example.com"},
            "bsky_fg": {"endpoint": "https://feedgen.example.com"},
        }
    }
    pds, labeler, chat, feedgen = extract_services(operation)
    assert pds == "https://pds.example.com"
    assert labeler == "https://labeler.example.com"
    assert chat == "https://chat.example.com"
    assert feedgen == "https://feedgen.example.com"


def test_extract_services_partial_services():
    operation = {
        "services": {
            "atproto_pds": {"endpoint": "https://pds.example.com"},
            "bsky_chat": {"endpoint": "https://chat.example.com"},
        }
    }
    pds, labeler, chat, feedgen = extract_services(operation)
    assert pds == "https://pds.example.com"
    assert labeler is None
    assert chat == "https://chat.example.com"
    assert feedgen is None


def test_extract_services_fallback_to_service_field():
    operation = {"service": "https://legacy-pds.example.com"}
    pds, labeler, chat, feedgen = extract_services(operation)
    assert pds == "https://legacy-pds.example.com"
    assert labeler is None
    assert chat is None
    assert feedgen is None


def test_extract_services_prefers_services_over_service():
    operation = {
        "services": {
            "atproto_pds": {"endpoint": "https://new-pds.example.com"},
        },
        "service": "https://old-pds.example.com",
    }
    pds, _labeler, _chat, _feedgen = extract_services(operation)
    assert pds == "https://new-pds.example.com"


def test_extract_services_empty_operation():
    pds, labeler, chat, feedgen = extract_services({})
    assert pds is None
    assert labeler is None
    assert chat is None
    assert feedgen is None


def test_extract_services_empty_services():
    pds, labeler, chat, feedgen = extract_services({"services": {}})
    assert pds is None
    assert labeler is None
    assert chat is None
    assert feedgen is None


def test_extract_services_missing_endpoint_field():
    operation = {
        "services": {
            "atproto_pds": {},
            "atproto_labeler": {"endpoint": "https://labeler.example.com"},
        }
    }
    pds, labeler, chat, feedgen = extract_services(operation)
    assert pds is None
    assert labeler == "https://labeler.example.com"


# --- extract_verification_keys ---


def test_extract_verification_keys_both_keys():
    operation = {
        "verificationMethods": {
            "atproto": "did:key:atproto123",
            "atproto_label": "did:key:labeler456",
        }
    }
    atproto_key, labeler_key = extract_verification_keys(operation)
    assert atproto_key == "did:key:atproto123"
    assert labeler_key == "did:key:labeler456"


def test_extract_verification_keys_only_atproto():
    operation = {"verificationMethods": {"atproto": "did:key:atproto"}}
    atproto_key, labeler_key = extract_verification_keys(operation)
    assert atproto_key == "did:key:atproto"
    assert labeler_key is None


def test_extract_verification_keys_fallback_to_signing_key():
    operation = {"signingKey": "did:key:signing"}
    atproto_key, labeler_key = extract_verification_keys(operation)
    assert atproto_key == "did:key:signing"
    assert labeler_key is None


def test_extract_verification_keys_prefers_verification_methods():
    operation = {
        "verificationMethods": {"atproto": "did:key:verification"},
        "signingKey": "did:key:signing",
    }
    atproto_key, labeler_key = extract_verification_keys(operation)
    assert atproto_key == "did:key:verification"


def test_extract_verification_keys_empty_operation():
    atproto_key, labeler_key = extract_verification_keys({})
    assert atproto_key is None
    assert labeler_key is None


def test_extract_verification_keys_empty_verification_methods():
    operation = {"verificationMethods": {}}
    atproto_key, labeler_key = extract_verification_keys(operation)
    assert atproto_key is None
    assert labeler_key is None


def test_extract_verification_keys_only_labeler_key():
    operation = {"verificationMethods": {"atproto_label": "did:key:labeler"}}
    atproto_key, labeler_key = extract_verification_keys(operation)
    assert atproto_key is None
    assert labeler_key == "did:key:labeler"
