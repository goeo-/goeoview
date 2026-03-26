from unittest.mock import MagicMock, patch

import pytest

from goeoview.helpers import sb32e
from goeoview.relay import parse_repo


def _make_user():
    user = MagicMock()
    user.did = "did:plc:test123"
    user.handle = "test.bsky.social"
    user.atproto_key = "did:key:test"
    return user


def _mock_valid_signature(mock_parse_pubkey):
    mock_verifier = MagicMock()
    mock_verifier.verify.return_value = None
    mock_parse_pubkey.return_value = mock_verifier


class TestParseRepo:
    @patch("goeoview.relay.carfile")
    def test_parse_repo_car_parse_error(self, mock_carfile):
        mock_carfile.parse.side_effect = ValueError("invalid CAR")

        result = parse_repo(_make_user(), b"invalid")

        assert result == ("did:plc:test123", [])
        mock_carfile.parse.assert_called_once_with(b"invalid")

    @patch("goeoview.relay.mst")
    @patch("goeoview.relay.parse_pubkey")
    @patch("goeoview.relay.carfile")
    def test_parse_repo_success_no_differences(
        self, mock_carfile, mock_parse_pubkey, mock_mst
    ):
        user = _make_user()
        rev = sb32e(b"\x00\x00\x00\x00\x00\x00\x00\x05")
        root_cid = "root_cid"
        data_cid = "data_cid"

        mock_carfile.parse.return_value = (
            {"roots": [root_cid]},
            {
                root_cid: {"rev": rev, "did": user.did, "sig": b"sig", "data": data_cid},
                data_cid: {"test": "data"},
            },
        )
        _mock_valid_signature(mock_parse_pubkey)
        mock_mst.mst_dfs_dump.return_value = ([], None, None)

        result = parse_repo(user, b"car")

        assert result == (user.did, [])

    @patch("goeoview.relay.mst")
    @patch("goeoview.relay.parse_pubkey")
    @patch("goeoview.relay.carfile")
    def test_parse_repo_with_new_records(
        self, mock_carfile, mock_parse_pubkey, mock_mst
    ):
        user = _make_user()
        rev = sb32e(b"\x00\x00\x00\x00\x00\x00\x00\x0a")
        root_cid = "root_cid"
        data_cid = "data_cid"
        record_cid = "record_cid"

        mock_carfile.parse.return_value = (
            {"roots": [root_cid]},
            {
                root_cid: {"rev": rev, "did": user.did, "sig": b"sig", "data": data_cid},
                data_cid: {"test": "data"},
                record_cid: {"text": "Hello World", "createdAt": "2024-01-01T00:00:00Z"},
            },
        )
        _mock_valid_signature(mock_parse_pubkey)
        mock_mst.mst_dfs_dump.return_value = (
            [(b"app.bsky.feed.post/abc123", record_cid)],
            None,
            None,
        )

        result = parse_repo(user, b"car")

        assert result[0] == user.did
        assert result[1] == [
            (
                user.did,
                "app.bsky.feed.post",
                "abc123",
                10,
                '{"text":"Hello World","createdAt":"2024-01-01T00:00:00Z"}',
            )
        ]

    @patch("goeoview.relay.mst")
    @patch("goeoview.relay.parse_pubkey")
    @patch("goeoview.relay.carfile")
    def test_parse_repo_with_record_differences(
        self, mock_carfile, mock_parse_pubkey, mock_mst
    ):
        user = _make_user()
        rev = sb32e(b"\x00\x00\x00\x00\x00\x00\x00\x14")
        root_cid = "root_cid"
        data_cid = "data_cid"
        record_cid = "record_cid"

        mock_carfile.parse.return_value = (
            {"roots": [root_cid]},
            {
                root_cid: {"rev": rev, "did": user.did, "sig": b"sig", "data": data_cid},
                data_cid: {"test": "data"},
                record_cid: {"text": "Updated text", "createdAt": "2024-01-01T00:00:00Z"},
            },
        )
        _mock_valid_signature(mock_parse_pubkey)
        mock_mst.mst_dfs_dump.return_value = (
            [(b"app.bsky.feed.post/abc123", record_cid)],
            None,
            None,
        )

        result = parse_repo(user, b"car")

        assert len(result[1]) == 1
        assert result[1][0][:4] == (
            user.did,
            "app.bsky.feed.post",
            "abc123",
            20,
        )

    @patch("goeoview.relay.mst")
    @patch("goeoview.relay.parse_pubkey")
    @patch("goeoview.relay.carfile")
    def test_parse_repo_invalid_signature(
        self, mock_carfile, mock_parse_pubkey, mock_mst
    ):
        from cryptography.exceptions import InvalidSignature

        user = _make_user()
        rev = sb32e(b"\x00\x00\x00\x00\x00\x00\x00\x05")
        root_cid = "root_cid"
        data_cid = "data_cid"

        mock_carfile.parse.return_value = (
            {"roots": [root_cid]},
            {
                root_cid: {"rev": rev, "did": user.did, "sig": b"bad", "data": data_cid},
                data_cid: {"test": "data"},
            },
        )
        mock_verifier = MagicMock()
        mock_verifier.verify.side_effect = InvalidSignature("bad signature")
        mock_parse_pubkey.return_value = mock_verifier
        mock_mst.mst_dfs_dump.return_value = ([], None, None)

        with pytest.raises(InvalidSignature):
            parse_repo(user, b"car")

    @patch("goeoview.relay.mst")
    @patch("goeoview.relay.parse_pubkey")
    @patch("goeoview.relay.carfile")
    def test_parse_repo_record_not_in_payload(
        self, mock_carfile, mock_parse_pubkey, mock_mst
    ):
        user = _make_user()
        rev = sb32e(b"\x00\x00\x00\x00\x00\x00\x00\x05")
        root_cid = "root_cid"
        data_cid = "data_cid"

        mock_carfile.parse.return_value = (
            {"roots": [root_cid]},
            {
                root_cid: {"rev": rev, "did": user.did, "sig": b"sig", "data": data_cid},
                data_cid: {"test": "data"},
            },
        )
        _mock_valid_signature(mock_parse_pubkey)
        mock_mst.mst_dfs_dump.return_value = (
            [(b"app.bsky.feed.post/abc123", "missing_cid")],
            None,
            None,
        )

        with pytest.raises(KeyError):
            parse_repo(user, b"car")

    @patch("goeoview.relay.mst")
    @patch("goeoview.relay.parse_pubkey")
    @patch("goeoview.relay.carfile")
    def test_parse_repo_no_rev_field(self, mock_carfile, mock_parse_pubkey, mock_mst):
        user = _make_user()
        root_cid = "root_cid"
        data_cid = "data_cid"

        mock_carfile.parse.return_value = (
            {"roots": [root_cid]},
            {
                root_cid: {"did": user.did, "sig": b"sig", "data": data_cid},
                data_cid: {"test": "data"},
            },
        )
        _mock_valid_signature(mock_parse_pubkey)
        mock_mst.mst_dfs_dump.return_value = ([], None, None)

        with pytest.raises(KeyError):
            parse_repo(user, b"car")
