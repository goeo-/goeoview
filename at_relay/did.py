from async_lru import alru_cache
import httpx
import orjson

import asyncio
from datetime import datetime, UTC

from .db import DB


client = httpx.AsyncClient()

# todo make sure did_id mv is before follow/block mvs
# todo also add first level mv to did_id from follow/block targets
# todo potentially also like targets, post replied tos, etc

@alru_cache(maxsize=10240, ttl=60 * 5)
async def resolve(did, weak=False, retries=0):
    db = DB()
    if did.startswith("did:plc:"):
        ret = await db.db.query(
            "SELECT handle, pds, labeler, chat, feedgen, atproto_key, labeler_key FROM plc_latest WHERE did = {did:String} ORDER BY created_at DESC LIMIT 1",
            {"did": did},
        )

        try:
            ret = ret.first_row
        except IndexError:
            if not weak:
                await asyncio.sleep(1)
                if retries < 5:
                    return await resolve(did, retries=retries + 1)
                else:
                    raise Exception("plc not found", did)

            ret = await client.get(f"https://plc.directory/{did}")
            if ret.status_code != 200:
                raise Exception("plc not found", did)

            return parse_did_doc(ret.json())

        return AtprotoUser(
            did=did,
            pds=ret[1],
            handle=ret[0],
            labeler=ret[2],
            chat=ret[3],
            feedgen=ret[4],
            atproto_key=ret[5][8:] if ret[5] else None,
            labeler_key=ret[6][8:] if ret[6] else None,
        )

    elif did.startswith("did:web:"):
        ret = await client.get(f"https://{did[8:]}/.well-known/did.json")
        if ret.status_code != 200:
            raise Exception("web not found", did)
        
        did_doc = ret.text

        ret = await db.db.query('select doc from did_web where did={did:String} order by fetched_at desc limit 1', {"did": did})
        try:
            old_doc = ret.first_row[0]
        except IndexError:
            old_doc = ''

        if old_doc != did_doc:
            await db.db.insert('did_web', [(
                did,
                datetime.now(UTC),
                did_doc,
            )], settings={'async_insert': True})

        ret = parse_did_doc(orjson.loads(did_doc))
        # todo insert?
        assert ret.did == did
        return ret

    else:
        raise Exception("invalid did", did)


class AtprotoUser:
    __slots__ = [
        "did",
        "pds",
        "handle",
        "labeler",
        "chat",
        "feedgen",
        "atproto_key",
        "labeler_key",
    ]

    def __init__(
        self, did, pds, handle, labeler, chat, feedgen, atproto_key, labeler_key
    ):
        self.did = did
        self.pds = pds
        self.handle = handle
        self.labeler = labeler
        self.chat = chat
        self.feedgen = feedgen

        self.atproto_key = atproto_key  # parse_pubkey(atproto_key)
        self.labeler_key = labeler_key  # parse_pubkey(labeler_key)

    def __str__(self) -> str:
        return ", ".join([f"{slot}: {getattr(self, slot)}" for slot in self.__slots__])


def parse_did_doc(doc):
    did = doc["id"]

    handle = None
    for aka in doc.get("alsoKnownAs", []):
        if aka.startswith("at://"):
            handle = aka[5:]
            break

    pds = None
    labeler = None
    chat = None
    feedgen = None
    for service in doc.get("service", []):
        if (
            pds is None
            and service.get("id") == "#atproto_pds"
            and service.get("type") == "AtprotoPersonalDataServer"
        ):
            pds = service.get("serviceEndpoint")
        elif (
            labeler is None
            and service.get("id") == "#atproto_labeler"
            and service.get("type") == "AtprotoLabeler"
        ):
            labeler = service.get("serviceEndpoint")
        elif (
            chat is None
            and service.get("id") == "#bsky_chat"
            and service.get("type") == "BskyChatService"
        ):
            chat = service.get("serviceEndpoint")
        elif (
            feedgen is None
            and service.get("id") == "#bsky_fg"
            and service.get("type") == "BskyFeedGenerator"
        ):
            feedgen = service.get("serviceEndpoint")

    atproto_key = None
    labeler_key = None
    for verification_method in doc.get("verificationMethod", []):
        if (
            atproto_key is None
            and verification_method.get("id") == f"{did}#atproto"
            and verification_method.get("type") == "Multikey"
            and verification_method.get("controller") == did
        ):
            atproto_key = verification_method.get("publicKeyMultibase")
        elif (
            labeler_key is None
            and verification_method.get("id") == f"{did}#atproto_label"
            and verification_method.get("type") == "Multikey"
            and verification_method.get("controller") == did
        ):
            labeler_key = verification_method.get("publicKeyMultibase")

    return AtprotoUser(
        did=did,
        pds=pds,
        handle=handle,
        labeler=labeler,
        chat=chat,
        feedgen=feedgen,
        atproto_key=atproto_key,
        labeler_key=labeler_key,
    )
