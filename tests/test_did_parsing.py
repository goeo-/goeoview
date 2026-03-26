"""Tests for DID document parsing and AtprotoUser class."""

from goeoview.did import AtprotoUser, parse_did_doc


class TestParseDIDDoc:
    """Test DID document parsing."""

    def test_parse_did_doc_complete(self):
        """Test parsing a complete DID document with all fields."""
        doc = {
            "id": "did:plc:test123",
            "alsoKnownAs": ["at://test.bsky.social"],
            "service": [
                {
                    "id": "#atproto_pds",
                    "type": "AtprotoPersonalDataServer",
                    "serviceEndpoint": "https://pds.example.com"
                },
                {
                    "id": "#atproto_labeler",
                    "type": "AtprotoLabeler",
                    "serviceEndpoint": "https://labeler.example.com"
                },
                {
                    "id": "#bsky_chat",
                    "type": "BskyChatService",
                    "serviceEndpoint": "https://chat.example.com"
                },
                {
                    "id": "#bsky_fg",
                    "type": "BskyFeedGenerator",
                    "serviceEndpoint": "https://feedgen.example.com"
                }
            ],
            "verificationMethod": [
                {
                    "id": "did:plc:test123#atproto",
                    "type": "Multikey",
                    "controller": "did:plc:test123",
                    "publicKeyMultibase": "zAtprotoKey123"
                },
                {
                    "id": "did:plc:test123#atproto_label",
                    "type": "Multikey",
                    "controller": "did:plc:test123",
                    "publicKeyMultibase": "zLabelerKey456"
                }
            ]
        }

        user = parse_did_doc(doc)

        assert user.did == "did:plc:test123"
        assert user.handle == "test.bsky.social"
        assert user.pds == "https://pds.example.com"
        assert user.labeler == "https://labeler.example.com"
        assert user.chat == "https://chat.example.com"
        assert user.feedgen == "https://feedgen.example.com"
        assert user.atproto_key == "zAtprotoKey123"
        assert user.labeler_key == "zLabelerKey456"

    def test_parse_did_doc_minimal(self):
        """Test parsing a minimal DID document."""
        doc = {
            "id": "did:plc:minimal",
        }

        user = parse_did_doc(doc)

        assert user.did == "did:plc:minimal"
        assert user.handle is None
        assert user.pds is None
        assert user.labeler is None
        assert user.chat is None
        assert user.feedgen is None
        assert user.atproto_key is None
        assert user.labeler_key is None


class TestAtprotoUser:
    """Test AtprotoUser class."""

    def test_atproto_user_initialization(self):
        """Test AtprotoUser initialization."""
        user = AtprotoUser(
            did="did:plc:test",
            pds="https://pds.example.com",
            handle="test.handle",
            labeler=None,
            chat=None,
            feedgen=None,
            atproto_key="key123",
            labeler_key=None,
        )

        assert user.did == "did:plc:test"
        assert user.pds == "https://pds.example.com"
        assert user.handle == "test.handle"
        assert user.atproto_key == "key123"

    def test_atproto_user_str(self):
        """Test AtprotoUser string representation."""
        user = AtprotoUser(
            did="did:plc:test",
            pds="https://pds.example.com",
            handle="test.handle",
            labeler=None,
            chat=None,
            feedgen=None,
            atproto_key="key123",
            labeler_key=None,
        )

        str_repr = str(user)
        assert "did:plc:test" in str_repr
        assert "test.handle" in str_repr
        assert "https://pds.example.com" in str_repr

    def test_atproto_user_slots(self):
        """Test AtprotoUser uses __slots__ for memory efficiency."""
        assert hasattr(AtprotoUser, "__slots__")
        assert "did" in AtprotoUser.__slots__
        assert "pds" in AtprotoUser.__slots__
        assert "handle" in AtprotoUser.__slots__
        assert "atproto_key" in AtprotoUser.__slots__

    def test_atproto_user_all_none(self):
        """Test AtprotoUser with all None optional fields."""
        user = AtprotoUser(
            did="did:plc:bare",
            pds=None,
            handle=None,
            labeler=None,
            chat=None,
            feedgen=None,
            atproto_key=None,
            labeler_key=None,
        )

        assert user.did == "did:plc:bare"
        assert user.pds is None
        assert user.handle is None
        assert user.labeler is None
        assert user.chat is None
        assert user.feedgen is None
        assert user.atproto_key is None
        assert user.labeler_key is None
