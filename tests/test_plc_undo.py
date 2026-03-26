"""Regression test for PLC undo 72-hour window check.

The bug: the 72h check compared the new op's created_at against the
prev target's created_at, instead of against the oldest undone op's
created_at. This caused valid undos to be rejected when the prev target
was old but the undone ops were recent.
"""

import base64
import hashlib

import cbrrr
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
from multiformats import multibase, multicodec

from goeoview.helpers import b32e
from goeoview.plc import parse_plc_op


SECP256K1_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141


def make_key():
    private = ec.generate_private_key(ec.SECP256K1())
    compressed = private.public_key().public_bytes(
        serialization.Encoding.X962,
        serialization.PublicFormat.CompressedPoint,
    )
    did_key = "did:key:" + multibase.encode(
        multicodec.wrap("secp256k1-pub", compressed), "base58btc"
    )
    return private, did_key


def sign_op(private_key, operation):
    unsigned = {k: v for k, v in operation.items() if k != "sig"}
    cbor = cbrrr.encode_dag_cbor(unsigned)
    der_sig = private_key.sign(cbor, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(der_sig)
    if s > SECP256K1_N // 2:
        s = SECP256K1_N - s
    raw = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def make_cid(operation):
    cbor = cbrrr.encode_dag_cbor(operation)
    digest = hashlib.sha256(cbor).digest()
    cid_bytes = b"\x01\x71\x12\x20" + digest
    return "b" + base64.b32encode(cid_bytes).decode().lower().strip("="), digest


def make_record(private_key, did_key, did, prev_cid, created_at, rotation_keys=None):
    operation = {
        "type": "plc_operation",
        "prev": prev_cid,
        "rotationKeys": rotation_keys or [did_key],
        "verificationMethods": {"atproto": did_key},
        "alsoKnownAs": ["at://test.example"],
        "services": {
            "atproto_pds": {
                "type": "AtprotoPersonalDataServer",
                "endpoint": "https://pds.example.com",
            }
        },
    }
    operation["sig"] = sign_op(private_key, operation)
    cid, digest = make_cid(operation)
    if did is None:
        did = "did:plc:" + b32e(digest[:15])
    return {
        "did": did,
        "cid": cid,
        "createdAt": created_at,
        "operation": operation,
    }


def make_prev_row(record, signed_by=0):
    """Build a prev_ops row tuple from a record, matching fetch_prev_records format."""
    from datetime import datetime

    return (
        datetime.fromisoformat(record["createdAt"]),
        record["cid"],
        record["operation"]["rotationKeys"],
        signed_by,
    )


def test_undo_within_72h_with_old_prev_target():
    """Undo of a recent op should succeed even if the prev target is old.

    Regression: the 72h check compared against the prev target's created_at
    instead of the undone op's created_at, causing this case to be rejected.
    """
    private, did_key = make_key()
    _, extra_did_key = make_key()
    two_keys = [did_key, extra_did_key]

    genesis = make_record(private, did_key, None, None, "2025-01-01T00:00:00.000Z", two_keys)
    did = genesis["did"]

    # Op 1 (prev target): 100 days ago
    op1 = make_record(private, did_key, did, genesis["cid"], "2025-04-01T00:00:00.000Z", two_keys)

    # Op 2 (will be undone, signed by key index 1): 1 hour ago
    op2 = make_record(private, did_key, did, op1["cid"], "2025-09-25T20:00:00.000Z", two_keys)

    # Op 3 (the undo, signed by key index 0): now, references op1's cid
    op3 = make_record(private, did_key, did, op1["cid"], "2025-09-25T21:00:00.000Z", two_keys)

    # prev_ops ordered DESC; op2 signed by key 1 so undo can use key 0
    prev_ops = [make_prev_row(op2, signed_by=1), make_prev_row(op1), make_prev_row(genesis)]

    result = parse_plc_op(op3, prev_ops)
    assert result is not None


def test_undo_exceeding_72h_still_rejected():
    """Undo should be rejected when the undone op is older than 72 hours."""
    private, did_key = make_key()
    _, extra_did_key = make_key()
    two_keys = [did_key, extra_did_key]

    genesis = make_record(private, did_key, None, None, "2025-01-01T00:00:00.000Z", two_keys)
    did = genesis["did"]

    op1 = make_record(private, did_key, did, genesis["cid"], "2025-04-01T00:00:00.000Z", two_keys)

    # Op 2 (will be undone): 4 days ago
    op2 = make_record(private, did_key, did, op1["cid"], "2025-09-21T00:00:00.000Z", two_keys)

    # Op 3 (the undo): now, references op1's cid, skipping op2
    op3 = make_record(private, did_key, did, op1["cid"], "2025-09-25T21:00:00.000Z", two_keys)

    prev_ops = [make_prev_row(op2, signed_by=1), make_prev_row(op1), make_prev_row(genesis)]

    with pytest.raises(Exception, match="PLC undo exceeded 72 hour window"):
        parse_plc_op(op3, prev_ops)


def test_normal_update_not_affected():
    """A normal update (prev points to most recent op) should not trigger undo check."""
    private, did_key = make_key()

    genesis = make_record(private, did_key, None, None, "2025-01-01T00:00:00.000Z")
    did = genesis["did"]

    op1 = make_record(private, did_key, did, genesis["cid"], "2025-04-01T00:00:00.000Z")

    # Normal update pointing to most recent op, even though it's old
    op2 = make_record(private, did_key, did, op1["cid"], "2025-09-25T21:00:00.000Z")

    prev_ops = [make_prev_row(op1), make_prev_row(genesis)]

    result = parse_plc_op(op2, prev_ops)
    assert result is not None
