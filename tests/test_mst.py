"""Tests for MST (Merkle Search Tree) functions.

Tests for goeoview/mst.py focusing on:
- count_zeroes() edge cases
- mst_dfs() error paths and edge cases
- mst_dfs_dump() comprehensive coverage
"""

import hashlib

import pytest

from goeoview.mst import count_zeroes, mst_dfs, mst_dfs_dump


class TestCountZeroes:
    """Test count_zeroes() function."""

    def test_all_zero_bytes(self):
        """Test count_zeroes() with all zero bytes."""
        data = b"\x00\x00\x00\x00"
        result = count_zeroes(data)
        assert result == 32  # 4 bytes * 8 bits = 32 zero bits

    def test_no_zero_bits(self):
        """Test count_zeroes() with no leading zero bits."""
        data = b"\xff\xff\xff\xff"
        result = count_zeroes(data)
        assert result == 0

    def test_partial_zeros(self):
        """Test count_zeroes() with partial leading zeros."""
        data = b"\x00\x0f\xff\xff"  # First 12 bits are zero
        result = count_zeroes(data)
        assert result == 12

    def test_single_byte_all_zero(self):
        """Test count_zeroes() with single zero byte."""
        data = b"\x00"
        result = count_zeroes(data)
        assert result == 8

    def test_single_byte_partial(self):
        """Test count_zeroes() with single byte partial zeros."""
        data = b"\x07"  # Binary: 00000111 - 5 leading zeros
        result = count_zeroes(data)
        assert result == 5

    def test_empty_bytes(self):
        """Test count_zeroes() with empty bytes."""
        data = b""
        result = count_zeroes(data)
        assert result == 0


class TestMSTDFS:
    """Test mst_dfs() function."""

    def test_target_v_none(self):
        """Test mst_dfs() with target_v=None (early return)."""
        tree = {}
        visited = set()
        result = mst_dfs(tree, visited, "root", None, None, 0)

        # Should return True immediately when target_v is None
        assert result == (True, None, None, None, 0)

    def test_start_none(self):
        """Test mst_dfs() with start=None."""
        tree = {}
        visited = set()
        result = mst_dfs(tree, visited, None, "key", "value", 0)

        # Should return False when start is None
        assert result == (False, None, None, None, 0)

    def test_target_k_string(self):
        """Test mst_dfs() converts string target_k to bytes with hash."""
        tree = {
            "root": {
                "l": None,
                "e": [
                    {
                        "k": b"test",
                        "v": "test_cid",
                        "p": 0,
                        "t": None,
                    }
                ]
            }
        }
        visited = set()

        # Pass target_k as string - should be converted to (bytes, depth) tuple
        result = mst_dfs(tree, visited, "root", "test", "test_cid", 0)
        assert result[0] is True

    def test_visited_node_raises(self):
        """Test mst_dfs() raises exception for cyclic tree."""
        tree = {
            "root": {
                "l": None,
                "e": []
            }
        }
        visited = {"root"}  # Already visited

        with pytest.raises(Exception, match="MST cycle detected"):
            mst_dfs(tree, visited, "root", "key", "value", 0)

    def test_node_not_in_tree(self):
        """Test mst_dfs() when node not in tree."""
        tree = {}
        visited = set()

        result = mst_dfs(tree, visited, "missing", "key", "value", 0)
        assert result == (False, None, None, None, -1)

    def test_entries_out_of_order_raises(self):
        """Test mst_dfs() raises when entries are out of order."""
        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": b"zzz", "v": "cid1", "p": 0, "t": None},
                    {"k": b"aaa", "v": "cid2", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST"):
            mst_dfs(tree, visited, "root", None, "value", 0)

    def test_right_first_key_out_of_order(self):
        """Test mst_dfs() raises when right subtree first key <= current key."""
        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": b"mmm", "v": "val1", "p": 0, "t": "right"},
                ]
            },
            "right": {
                "l": None,
                "e": [
                    {"k": b"aaa", "v": "val2", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST"):
            mst_dfs(tree, visited, "root", b"key", "value", 0)

    def test_entries_with_different_depths(self):
        """Test mst_dfs() raises when entries have different hash depths."""
        key1 = b"key_0000"
        key2 = b"key_0001"

        # Verify they have different depths
        depth1 = count_zeroes(hashlib.sha256(key1).digest()) // 2
        depth2 = count_zeroes(hashlib.sha256(key2).digest()) // 2
        assert depth1 != depth2, "Keys must have different depths for this test"

        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": key1, "v": "val1", "p": 0, "t": None},
                    {"k": key2, "v": "val2", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST node has entries with different depths"):
            mst_dfs(tree, visited, "root", b"target", "value", 0)

    def test_left_depth_greater_than_current(self):
        """Test mst_dfs() raises when left depth >= current depth."""
        current_key = b"\xff\xff\xff"  # High bytes = low depth
        left_key = b"\x00\x00\x00test"  # Low bytes = high depth

        tree = {
            "root": {
                "l": "left",
                "e": [
                    {"k": current_key, "v": "val", "p": 0, "t": None},
                ]
            },
            "left": {
                "l": None,
                "e": [
                    {"k": left_key, "v": "left_val", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST"):
            mst_dfs(tree, visited, "root", b"target", "value", 0)

    def test_right_depth_greater_than_current(self):
        """Test mst_dfs() raises when right depth >= current depth."""
        current_key = b"\xff\xff\xff"  # High bytes = low depth
        right_key = b"\x00\x00\x00zzzz"  # Low bytes = high depth (must be > current_key)

        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": current_key, "v": "val", "p": 0, "t": "right"},
                ]
            },
            "right": {
                "l": None,
                "e": [
                    {"k": right_key, "v": "right_val", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST"):
            mst_dfs(tree, visited, "root", b"target", "value", 0)

    def test_right_depth_check_with_known_keys(self):
        """Test mst_dfs() raises when right_depth >= this_depth using keys with known depths."""
        current_key = b"k0001"  # depth 0
        right_key = b"k0023"  # depth 3

        # Verify depths
        current_depth = count_zeroes(hashlib.sha256(current_key).digest()) // 2
        right_depth = count_zeroes(hashlib.sha256(right_key).digest()) // 2
        assert current_depth == 0, f"Expected depth 0, got {current_depth}"
        assert right_depth == 3, f"Expected depth 3, got {right_depth}"

        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": current_key, "v": "val", "p": 0, "t": "right"},
                ]
            },
            "right": {
                "l": None,
                "e": [
                    {"k": right_key, "v": "right_val", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST"):
            mst_dfs(tree, visited, "root", b"target", "value", 0)

    def test_last_key_empty_bytes_set_to_none(self):
        """Test mst_dfs() sets last_key to None when it equals b''."""
        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": b"", "v": "val", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        result = mst_dfs(tree, visited, "root", None, None, 0)

        # result[2] is last_key - should be None, not b""
        assert result[2] is None

    def test_last_key_empty_bytes_with_target(self):
        """Test mst_dfs() sets last_key to None when last_key == b'' with a real target."""
        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": b"", "v": "val", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        # target_v != None so we don't hit early return; target_k=None so match succeeds
        result = mst_dfs(tree, visited, "root", None, "val", 0)

        assert result[0] is True
        assert result[2] is None  # last_key should be None after empty-bytes normalization

    def test_complex_valid_tree(self):
        """Test mst_dfs with a valid multi-level tree."""
        tree = {
            "root": {
                "l": "left",
                "e": [
                    {"k": b"mmm", "v": "middle", "p": 0, "t": "right"},
                ],
            },
            "left": {
                "l": None,
                "e": [
                    {"k": b"aaa", "v": "left_val", "p": 0, "t": None},
                ]
            },
            "right": {
                "l": None,
                "e": [
                    {"k": b"zzz", "v": "right_val", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        result = mst_dfs(tree, visited, "root", None, None, 0)
        assert result[0] is True


class TestMSTDFSDump:
    """Test mst_dfs_dump() function."""

    def test_start_none(self):
        """Test mst_dfs_dump() with start=None."""
        tree = {}
        visited = set()
        result = mst_dfs_dump(tree, visited, None, 0)

        assert result == ([], None, 0)

    def test_visited_node_raises(self):
        """Test mst_dfs_dump() raises exception for cyclic tree."""
        tree = {
            "root": {
                "l": None,
                "e": []
            }
        }
        visited = {"root"}

        with pytest.raises(Exception, match="MST cycle detected"):
            mst_dfs_dump(tree, visited, "root", 0)

    def test_node_not_in_tree(self):
        """Test mst_dfs_dump() when node not in tree."""
        tree = {}
        visited = set()

        result = mst_dfs_dump(tree, visited, "missing", 0)
        assert result == ([], None, -1)

    def test_simple_tree(self):
        """Test mst_dfs_dump() with simple tree structure."""
        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": b"key1", "v": "value1", "p": 0, "t": None},
                    {"k": b"key2", "v": "value2", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        result = mst_dfs_dump(tree, visited, "root", 0)
        values = result[0]

        assert len(values) == 2
        assert values[0] == (b"key1", "value1")
        assert values[1] == (b"key2", "value2")

    def test_entries_out_of_order_raises(self):
        """Test mst_dfs_dump() raises when entries are out of order."""
        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": b"zzz", "v": "cid1", "p": 0, "t": None},
                    {"k": b"aaa", "v": "cid2", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST"):
            mst_dfs_dump(tree, visited, "root", 0)

    def test_with_left_subtree(self):
        """Test mst_dfs_dump() with left subtree."""
        tree = {
            "root": {
                "l": "left_node",
                "e": [
                    {"k": b"key", "v": "root_val", "p": 0, "t": None},
                ]
            },
            "left_node": {
                "l": None,
                "e": [
                    {"k": b"aaa", "v": "left_val", "p": 0, "t": None},
                ]
            },
        }
        visited = set()

        result = mst_dfs_dump(tree, visited, "root", 0)
        values = result[0]

        # Should visit left subtree first, then root
        assert len(values) == 2
        assert values[0] == (b"aaa", "left_val")
        assert values[1] == (b"key", "root_val")

    def test_empty_tree(self):
        """Test mst_dfs_dump() with tree that has no entries."""
        tree = {
            "root": {
                "l": None,
                "e": []
            }
        }
        visited = set()

        result = mst_dfs_dump(tree, visited, "root", 0)
        values = result[0]

        assert len(values) == 0

    def test_last_key_empty_bytes(self):
        """Test mst_dfs_dump() handles last_key == b'' edge case."""
        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": b"key", "v": "value", "p": 0, "t": "right_node"},
                ]
            },
            "right_node": {
                "l": None,
                "e": []
            },
        }
        visited = set()

        result = mst_dfs_dump(tree, visited, "root", 0)
        assert result is not None

    def test_last_key_empty_bytes_set_to_none(self):
        """Test mst_dfs_dump() sets last_key to None when it equals b''."""
        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": b"", "v": "cid", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        result = mst_dfs_dump(tree, visited, "root", 0)
        assert result is not None

    def test_last_key_less_than_first_key_raises(self):
        """Test mst_dfs_dump() raises when last_key < first_key (malformed tree)."""
        tree = {
            "root": {
                "l": "left_node",
                "e": [
                    {"k": b"mmm", "v": "mid", "p": 0, "t": None},
                ]
            },
            "left_node": {
                "l": None,
                "e": [
                    {"k": b"zzz", "v": "should_be_last", "p": 0, "t": None},
                ]
            },
        }
        visited = set()

        with pytest.raises(Exception, match="MST"):
            mst_dfs_dump(tree, visited, "root", 0)

    def test_right_first_key_equal_to_parent_raises(self):
        """Test mst_dfs_dump() raises when right subtree first key equals current key."""
        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": b"mmm", "v": "cid1", "p": 0, "t": "right"},
                ]
            },
            "right": {
                "l": None,
                "e": [
                    {"k": b"mmm", "v": "cid2", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST"):
            mst_dfs_dump(tree, visited, "root", 0)

    def test_left_right_subtree_ordering_violation(self):
        """Test mst_dfs_dump() raises when left subtree contains out-of-order keys."""
        tree = {
            "root": {
                "l": "left",
                "e": [
                    {"k": b"aaa", "v": "cid", "p": 0, "t": None},
                ]
            },
            "left": {
                "l": None,
                "e": [
                    {"k": b"000", "v": "left_cid", "p": 0, "t": "left_right"},
                ]
            },
            "left_right": {
                "l": None,
                "e": [
                    {"k": b"zzz", "v": "bad_cid", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST"):
            mst_dfs_dump(tree, visited, "root", 0)

    def test_entries_different_depths(self):
        """Test mst_dfs_dump() raises when entries have different hash depths."""
        key1 = b"key_0000"
        key2 = b"key_0001"

        # Verify different depths
        depth1 = count_zeroes(hashlib.sha256(key1).digest()) // 2
        depth2 = count_zeroes(hashlib.sha256(key2).digest()) // 2
        assert depth1 != depth2, "Keys must have different depths for this test"

        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": key1, "v": "cid1", "p": 0, "t": None},
                    {"k": key2, "v": "cid2", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST node has entries with different depths"):
            mst_dfs_dump(tree, visited, "root", 0)

    def test_depth_ordering_violation(self):
        """Test mst_dfs_dump() raises when left/right depth >= current depth."""
        current_key = b"\xff\xff\xff"  # Low depth
        left_key = b"\x00\x00\x00aaa"  # High depth, alphabetically before current

        tree = {
            "root": {
                "l": "left",
                "e": [
                    {"k": current_key, "v": "cid", "p": 0, "t": None},
                ]
            },
            "left": {
                "l": None,
                "e": [
                    {"k": left_key, "v": "left_cid", "p": 0, "t": None},
                ]
            }
        }
        visited = set()

        with pytest.raises(Exception, match="MST"):
            mst_dfs_dump(tree, visited, "root", 0)

    def test_valid_multi_entry_tree(self):
        """Test mst_dfs_dump with entries that have the same hash depth."""
        # key_0001 and key_0002 both need the same depth for this to work
        tree = {
            "root": {
                "l": None,
                "e": [
                    {"k": b"key_0001", "v": "cid1", "p": 0, "t": None},
                    {"k": b"key_0002", "v": "cid2", "p": 0, "t": None},
                ],
            },
        }
        visited = set()

        result = mst_dfs_dump(tree, visited, "root", 0)
        values = result[0]

        assert len(values) == 2
        assert values[0] == (b"key_0001", "cid1")
        assert values[1] == (b"key_0002", "cid2")
