from goeoview.did import parse_did_doc


def test_parse_did_doc_extracts_services_and_keys():
    did = "did:web:example.com"
    doc = {
        "id": did,
        "alsoKnownAs": ["at://alice.example.com"],
        "service": [
            {
                "id": "#atproto_pds",
                "type": "AtprotoPersonalDataServer",
                "serviceEndpoint": "https://pds.example.com",
            },
            {
                "id": "#bsky_fg",
                "type": "BskyFeedGenerator",
                "serviceEndpoint": "https://feed.example.com",
            },
        ],
        "verificationMethod": [
            {
                "id": f"{did}#atproto",
                "type": "Multikey",
                "controller": did,
                "publicKeyMultibase": "zKey123",
            }
        ],
    }

    user = parse_did_doc(doc)
    assert user.did == did
    assert user.handle == "alice.example.com"
    assert user.pds == "https://pds.example.com"
    assert user.feedgen == "https://feed.example.com"
    assert user.atproto_key == "zKey123"
