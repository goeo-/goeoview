"""Tests for helper functions."""

import hashlib

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    Prehashed,
    decode_dss_signature,
)
from multiformats import multibase, multicodec

from goeoview.helpers import Key, b32d, b32e, parse_cid, parse_pubkey, sb32d, sb32e, ub64d


# --- base32 encode/decode ---


def test_b32_encode_decode():
    """Test base32 encoding and decoding."""
    original = b"Hello, World!"
    encoded = b32e(original)
    decoded = b32d(encoded)

    assert isinstance(encoded, str)
    assert decoded == original


def test_b32_decode_with_string():
    """Test base32 decoding with string input."""
    assert b32d("nbswy3dp") == b"hello"


def test_b32_decode_with_bytes():
    """Test base32 decoding with bytes input."""
    assert b32d(b"nbswy3dp") == b"hello"


def test_b32d_with_invalid_type():
    """Test b32d() raises exception for invalid type."""
    with pytest.raises(Exception, match="invalid type"):
        b32d(12345)  # type: ignore


def test_b32d_with_invalid_type_list():
    """Test b32d() raises exception for list type."""
    with pytest.raises(Exception, match="invalid type"):
        b32d([1, 2, 3])  # type: ignore


# --- sortable base32 ---


def test_sb32_sortable_base32():
    """Test sortable base32 encoding/decoding."""
    original = b"\x00\x01\x02\x03\x04"
    encoded = sb32e(original)
    decoded = sb32d(encoded)

    assert isinstance(encoded, str)
    assert decoded == original


def test_sb32d_with_uppercase():
    """Test sb32d() lowercases input."""
    result = sb32d("AAAAA")
    assert isinstance(result, bytes)


def test_sb32d_with_bytes():
    """Test sb32d() accepts bytes input."""
    result = sb32d(b"22222")
    assert isinstance(result, bytes)


# --- URL-safe base64 ---


def test_ub64d_urlsafe_base64():
    """Test URL-safe base64 decoding."""
    assert ub64d("aGVsbG8") == b"hello"


def test_ub64d_with_bytes():
    """Test URL-safe base64 decoding with bytes input."""
    assert ub64d(b"aGVsbG8") == b"hello"


def test_ub64d_with_invalid_type():
    """Test ub64d() raises exception for invalid type."""
    with pytest.raises(Exception, match="invalid type"):
        ub64d(12345)  # type: ignore


def test_ub64d_with_invalid_type_list():
    """Test ub64d() raises exception for list type."""
    with pytest.raises(Exception, match="invalid type"):
        ub64d([1, 2, 3])  # type: ignore


# --- parse_cid ---


def test_parse_cid():
    """Test CID parsing with a valid dag-cbor SHA-256 CID."""
    # CIDv1, dag-cbor (0x71), sha-256 (0x12), 32 bytes
    cid = "bafyreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"

    result = parse_cid(cid)
    assert isinstance(result, bytes)
    assert len(result) == 32


def test_parse_cid_requires_b_prefix():
    """Test that parse_cid raises ValueError without 'b' prefix."""
    with pytest.raises(ValueError, match="CID must be base32 multibase"):
        parse_cid("invalidcid")


def test_parse_cid_unsupported_codec():
    """Test that parse_cid raises ValueError for unsupported codec."""
    # Encode some bytes that don't start with the expected prefix
    raw = b"\x00\x00\x00\x00" + b"\x00" * 32
    encoded = "b" + b32e(raw)

    with pytest.raises(ValueError, match="unsupported CID codec"):
        parse_cid(encoded)


# --- parse_pubkey ---


def test_parse_pubkey_with_none():
    """Test that parse_pubkey returns None for None input."""
    assert parse_pubkey(None) is None


def test_parse_pubkey_secp256k1():
    """Test parse_pubkey() with a real SECP256K1 key."""
    private_key = ec.generate_private_key(ec.SECP256K1(), default_backend())
    public_key = private_key.public_key()

    pubkey_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.X962,
        format=serialization.PublicFormat.UncompressedPoint,
    )
    wrapped = multicodec.wrap("secp256k1-pub", pubkey_bytes)
    encoded = multibase.encode(wrapped, "base58btc")

    key = parse_pubkey(encoded)

    assert key is not None
    assert key.key_n == 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
    assert isinstance(key.key, ec.EllipticCurvePublicKey)


def test_parse_pubkey_p256():
    """Test parse_pubkey() with a real P-256 key."""
    private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
    public_key = private_key.public_key()

    pubkey_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.X962,
        format=serialization.PublicFormat.UncompressedPoint,
    )
    wrapped = multicodec.wrap("p256-pub", pubkey_bytes)
    encoded = multibase.encode(wrapped, "base58btc")

    key = parse_pubkey(encoded)

    assert key is not None
    assert key.key_n == 0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551
    assert isinstance(key.key, ec.EllipticCurvePublicKey)


def test_parse_pubkey_invalid_key_type():
    """Test parse_pubkey() raises for unsupported key type."""
    fake_data = b"\x00" * 32
    wrapped = multicodec.wrap("ed25519-pub", fake_data)
    encoded = multibase.encode(wrapped, "base58btc")

    with pytest.raises(Exception, match="invalid pubkey"):
        parse_pubkey(encoded)


# --- Key class ---


def test_key_initialization():
    """Test Key class initialization."""
    private_key = ec.generate_private_key(ec.SECP256K1(), default_backend())
    public_key = private_key.public_key()
    key_n = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141

    key = Key(public_key, key_n)

    assert key.key == public_key
    assert key.key_n == key_n


def test_key_verify_with_64_byte_signature():
    """Test Key.verify() with 64-byte r||s low-S signature."""
    private_key = ec.generate_private_key(ec.SECP256K1(), default_backend())
    public_key = private_key.public_key()
    key_n = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141

    key = Key(public_key, key_n)

    data = b"test message"
    sig = _sign_raw_low_s(private_key, data, key_n)

    key.verify(sig, data)  # Should not raise


def test_key_verify_rejects_der_signature():
    """DER-encoded signatures must be rejected per atproto spec."""
    private_key = ec.generate_private_key(ec.SECP256K1(), default_backend())
    public_key = private_key.public_key()
    key_n = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141

    key = Key(public_key, key_n)

    data = b"test message"
    signature_der = private_key.sign(data, ec.ECDSA(hashes.SHA256()))

    with pytest.raises(InvalidSignature, match="64-byte raw"):
        key.verify(signature_der, data)


def _sign_raw_low_s(private_key, data, key_n):
    """Sign data and return a 64-byte raw low-S signature."""
    signature_der = private_key.sign(data, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(signature_der)
    if s > key_n // 2:
        s = key_n - s
    return r.to_bytes(32, "big") + s.to_bytes(32, "big")


def test_key_verify_low_s_accepted():
    """Low-S 64-byte raw signatures must be accepted."""
    private_key = ec.generate_private_key(ec.SECP256K1(), default_backend())
    public_key = private_key.public_key()
    key_n = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141

    key = Key(public_key, key_n)
    data = b"test message"
    sig = _sign_raw_low_s(private_key, data, key_n)

    key.verify(sig, data)  # Should not raise


def test_key_verify_rejects_high_s():
    """High-S signatures must be rejected per atproto spec."""
    private_key = ec.generate_private_key(ec.SECP256K1(), default_backend())
    public_key = private_key.public_key()
    key_n = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141

    key = Key(public_key, key_n)
    data = b"test message"
    signature_der = private_key.sign(data, ec.ECDSA(hashes.SHA256()))

    r, s = decode_dss_signature(signature_der)
    if s <= key_n // 2:
        s = key_n - s
    signature_64 = r.to_bytes(32, "big") + s.to_bytes(32, "big")

    with pytest.raises(InvalidSignature, match="high-S"):
        key.verify(signature_64, data)


def test_key_verify_with_prehashed():
    """Test Key.verify() with prehashed=True using raw 64-byte signature."""
    private_key = ec.generate_private_key(ec.SECP256K1(), default_backend())
    public_key = private_key.public_key()
    key_n = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141

    key = Key(public_key, key_n)

    data = b"test message"
    data_hash = hashlib.sha256(data).digest()

    sig_der = private_key.sign(data_hash, ec.ECDSA(Prehashed(hashes.SHA256())))
    r, s = decode_dss_signature(sig_der)
    if s > key_n // 2:
        s = key_n - s
    sig_raw = r.to_bytes(32, "big") + s.to_bytes(32, "big")

    key.verify(sig_raw, data_hash, prehashed=True)  # Should not raise


def test_key_verify_invalid_signature():
    """Test Key.verify() raises InvalidSignature for wrong key."""
    private_key1 = ec.generate_private_key(ec.SECP256K1(), default_backend())
    private_key2 = ec.generate_private_key(ec.SECP256K1(), default_backend())
    public_key2 = private_key2.public_key()
    key_n = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141

    key = Key(public_key2, key_n)

    data = b"test message"
    sig = _sign_raw_low_s(private_key1, data, key_n)

    with pytest.raises(InvalidSignature):
        key.verify(sig, data)


def test_parse_pubkey_p256_signature_verification():
    """Test that a parsed P-256 key can verify signatures end-to-end."""
    private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
    public_key = private_key.public_key()
    p256_n = 0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551

    pubkey_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.X962,
        format=serialization.PublicFormat.UncompressedPoint,
    )
    wrapped = multicodec.wrap("p256-pub", pubkey_bytes)
    encoded = multibase.encode(wrapped, "base58btc")

    key = parse_pubkey(encoded)
    assert key is not None

    data = b"test message for p256"
    sig_der = private_key.sign(data, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(sig_der)
    if s > p256_n // 2:
        s = p256_n - s
    sig_raw = r.to_bytes(32, "big") + s.to_bytes(32, "big")

    key.verify(sig_raw, data)  # Should not raise
