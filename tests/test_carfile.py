"""Tests for CAR (Content Addressable aRchive) file parsing.

Tests the parse() function in carfile.py including:
- Valid CAR file parsing
- Error handling for malformed CAR files
- CID validation
- Checksum validation
- EOF handling
"""

from hashlib import sha256

import cbrrr
import pytest
from leb128 import u as leb

from goeoview.carfile import CARFileError, parse


def make_block(data_dict):
    """Build a length-prefixed CAR block from a dict."""
    block_data = cbrrr.encode_dag_cbor(data_dict)
    block_hash = sha256(block_data).digest()
    cid_bytes = b"\x01q\x12 " + block_hash
    block_with_cid = cid_bytes + block_data
    block_length = leb.encode(len(block_with_cid))
    return block_length + block_with_cid


def make_header(roots=None):
    """Build a length-prefixed CAR header."""
    if roots is None:
        roots = []
    header = {"version": 1, "roots": roots}
    header_bytes = cbrrr.encode_dag_cbor(header)
    header_length = leb.encode(len(header_bytes))
    return header_length + header_bytes


class TestCARFileParsing:
    """Test CAR file parsing with valid inputs."""

    def test_parse_valid_car_file(self):
        """Test parsing a valid CAR file with single block."""
        root_cid = cbrrr.CID.cidv1_dag_cbor_sha256_32_from(b"test")
        car_bytes = make_header([root_cid]) + make_block({"key": "value"})

        hdr, pld = parse(car_bytes)

        assert hdr["version"] == 1
        assert len(pld) == 1
        assert any(block["key"] == "value" for block in pld.values())

    def test_parse_car_multiple_blocks(self):
        """Test parsing CAR file with multiple blocks."""
        root_cid = cbrrr.CID.cidv1_dag_cbor_sha256_32_from(b"root")
        blocks = b"".join(
            make_block({"index": i, "data": f"block{i}"}) for i in range(3)
        )
        car_bytes = make_header([root_cid]) + blocks

        hdr, pld = parse(car_bytes)

        assert hdr["version"] == 1
        assert len(pld) == 3
        indices = sorted(block["index"] for block in pld.values())
        assert indices == [0, 1, 2]

    def test_parse_car_empty_blocks(self):
        """Test parsing CAR file with header but no blocks (EOF immediately)."""
        car_bytes = make_header()

        hdr, pld = parse(car_bytes)

        assert hdr["version"] == 1
        assert len(pld) == 0


class TestCARFileErrors:
    """Test CAR file error handling."""

    def test_parse_car_invalid_cid_prefix(self):
        """Test that parsing fails with invalid CID prefix."""
        header_bytes = make_header()

        # Create block with INVALID CID prefix (should be \x01q\x12\x20)
        block_data = cbrrr.encode_dag_cbor({"key": "value"})
        block_hash = sha256(block_data).digest()
        invalid_cid_bytes = b"\x00\x00\x00\x00" + block_hash
        block_with_cid = invalid_cid_bytes + block_data
        block_length = leb.encode(len(block_with_cid))

        car_bytes = header_bytes + block_length + block_with_cid

        with pytest.raises(CARFileError, match="unsupported CAR CID prefix"):
            parse(car_bytes)

    def test_parse_car_checksum_mismatch(self):
        """Test that parsing fails with checksum mismatch."""
        header_bytes = make_header()

        # Create block with WRONG checksum
        block_data = cbrrr.encode_dag_cbor({"key": "value"})
        wrong_hash = sha256(b"different data").digest()
        cid_bytes = b"\x01q\x12 " + wrong_hash
        block_with_cid = cid_bytes + block_data
        block_length = leb.encode(len(block_with_cid))

        car_bytes = header_bytes + block_length + block_with_cid

        with pytest.raises(CARFileError, match="CAR block hash mismatch"):
            parse(car_bytes)

    def test_parse_car_truncated_block(self):
        """Test that parsing fails when block data is too short for a CID."""
        header_bytes = make_header()

        # Create a block that's shorter than 36 bytes (minimum for CID)
        short_data = b"\x01q\x12 " + b"\x00" * 10  # only 14 bytes
        block_length = leb.encode(len(short_data))

        car_bytes = header_bytes + block_length + short_data

        with pytest.raises(CARFileError, match="truncated CAR block"):
            parse(car_bytes)


class TestCARFileEdgeCases:
    """Test edge cases in CAR file parsing."""

    def test_parse_car_large_block(self):
        """Test parsing CAR file with a large block."""
        large_data = b"x" * (1024 * 1024)
        car_bytes = make_header() + make_block({"data": large_data})

        hdr, pld = parse(car_bytes)

        assert len(pld) == 1
        assert len(list(pld.values())[0]["data"]) == 1024 * 1024

    def test_parse_car_nested_structures(self):
        """Test parsing CAR file with deeply nested CBOR structures."""
        nested = {"level": 0}
        for i in range(1, 10):
            nested = {"level": i, "nested": nested}

        car_bytes = make_header() + make_block(nested)

        hdr, pld = parse(car_bytes)

        assert len(pld) == 1
        parsed_block = list(pld.values())[0]
        assert parsed_block["level"] == 9
        assert parsed_block["nested"]["level"] == 8
