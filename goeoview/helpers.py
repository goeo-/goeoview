from multiformats import multicodec, multibase

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    encode_dss_signature,
    Prehashed,
)

from base64 import b32decode, b32encode, urlsafe_b64decode

class Key:
    __slots__ = ("key", "key_n")

    def __init__(self, key, key_n) -> None:
        self.key = key
        self.key_n = key_n

    def verify(self, signature, data, prehashed=False):
        if len(signature) != 64:
            raise InvalidSignature("signature must be 64-byte raw (r||s) format")
        r = int.from_bytes(signature[:32], "big")
        s = int.from_bytes(signature[32:], "big")
        if s > self.key_n // 2:
            raise InvalidSignature("high-S signature")
        self.key.verify(
            encode_dss_signature(r, s),
            data,
            ec.ECDSA(Prehashed(hashes.SHA256()) if prehashed else hashes.SHA256()),
        )


normal = b"abcdefghijklmnopqrstuvwxyz234567"
sortable = b"234567abcdefghijklmnopqrstuvwxyz"

def b32d(e):
    if isinstance(e, str):
        return b32decode(e.upper() + "=" * (-len(e) % 8))
    elif isinstance(e, bytes):
        return b32decode(e.upper() + b"=" * (-len(e) % 8))
    raise Exception("invalid type", e)


def sb32d(e):
    e = e.lower()
    if isinstance(e, str):
        e = e.encode()

    return b32d(bytes([normal[sortable.index(x)] for x in e]))


def b32e(d):
    return b32encode(d).decode().lower().strip("=")

def sb32e(d):
    return bytes([sortable[normal.index(x)] for x in b32encode(d).lower().strip(b"=")]).decode()


def ub64d(e):
    if isinstance(e, str):
        return urlsafe_b64decode(e + "=" * (-len(e) % 4))
    elif isinstance(e, bytes):
        return urlsafe_b64decode(e + b"=" * (-len(e) % 4))
    raise Exception("invalid type", e)


def parse_pubkey(key):
    if key is None:
        return None

    key_bytes = multibase.decode(key)
    key_type, key_data = multicodec.unwrap(key_bytes)

    if key_type.name == "secp256k1-pub":
        key_n = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
        pub_key = ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256K1(), key_data)
    elif key_type.name == "p256-pub":
        key_n = 0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551
        pub_key = ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), key_data)
    else:
        raise Exception("invalid pubkey", key_type, key_data)

    return Key(pub_key, key_n)


def parse_cid(cid):
    if not cid.startswith("b"):
        raise ValueError("CID must be base32 multibase")
    cid_h = b32d(cid[1:])
    if not cid_h.startswith(b"\x01q\x12 "):
        raise ValueError("unsupported CID codec")
    return cid_h[4:]
