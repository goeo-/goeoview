from async_lru import alru_cache
import orjson

import asyncio
from datetime import datetime, UTC

from .db import DB
from .safe_transport import safe_get

# todo make sure did_id mv is before follow/block mvs
# todo also add first level mv to did_id from follow/block targets
# todo potentially also like targets, post replied tos, etc


class DIDResolver:
    def __init__(self, http_clients, plc_state_store):
        self._http_clients = http_clients
        self._plc_state_store = plc_state_store
        self.resolve = alru_cache(maxsize=10240, ttl=60 * 5)(self._resolve)

    async def _resolve(self, did, weak=False, retries=0):
        db = DB()
        if did.startswith("did:plc:"):
            cached = await self._plc_state_store.get_latest(did)
            if cached is not None:
                return AtprotoUser(
                    did=cached["did"],
                    pds=cached["pds"],
                    handle=cached["handle"],
                    labeler=cached["labeler"],
                    chat=cached["chat"],
                    feedgen=cached["feedgen"],
                    atproto_key=cached["atproto_key"][8:] if cached["atproto_key"] else None,
                    labeler_key=cached["labeler_key"][8:] if cached["labeler_key"] else None,
                )

            if not weak:
                await asyncio.sleep(1)
                if retries < 5:
                    return await self.resolve(did, retries=retries + 1)
                weak = True

            async with self._http_clients.trusted().get(f"https://plc.directory/{did}") as ret:
                if ret.status != 200:
                    raise Exception("plc not found", did)
                return parse_did_doc(await ret.json())

        if did.startswith("did:web:"):
            ret = await safe_get(
                self._http_clients.untrusted(),
                f"https://{did[8:]}/.well-known/did.json",
            )
            if ret.status != 200:
                raise Exception("web not found", did)

            did_doc = ret.text

            ret_db = await db.db.query('select doc from did_web where did={did:String} order by fetched_at desc limit 1', {"did": did})
            try:
                old_doc = ret_db.first_row[0]
            except IndexError:
                old_doc = ''

            if old_doc != did_doc:
                await db.db.insert('did_web', [(
                    did,
                    datetime.now(UTC),
                    did_doc,
                )], settings={'async_insert': True})

            ret_parsed = parse_did_doc(orjson.loads(did_doc))
            # todo insert?
            if ret_parsed.did != did:
                raise Exception("did:web document ID mismatch", did, ret_parsed.did)
            return ret_parsed

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
