"""CPU-bound commit validation for ProcessPoolExecutor workers.

This module contains only module-level functions (picklable) and must not
use async, loggers with handlers, or references to main-process singletons.
"""

import cbrrr
import orjson

from goeoview import carfile, mst
from goeoview.helpers import sb32d, parse_pubkey, InvalidSignature


def validate_commit(message_bytes, atproto_key):
    repo = None
    seq = None
    try:
        decoded = cbrrr.decode_dag_cbor(message_bytes)
        repo = decoded.get("repo")
        seq = decoded.get("seq")

        if decoded["tooBig"]:
            return {"ok": True, "rows": [], "repo": None}

        if atproto_key is None:
            return {"ok": False, "retry": True, "repo": repo, "seq": seq}

        blocks = decoded["blocks"]
        hdr, pld = carfile.parse(blocks)

        if len(hdr["roots"]) != 1:
            return {
                "ok": False, "fatal": True,
                "error": "commit CAR must have exactly one root",
                "repo": repo, "seq": seq,
            }
        if hdr["roots"][0] != decoded["commit"]:
            return {
                "ok": False, "fatal": True,
                "error": "commit root CID mismatch",
                "repo": repo, "seq": seq,
            }

        root = pld[hdr["roots"][0]]

        if root["rev"] != decoded["rev"]:
            return {
                "ok": False, "fatal": True,
                "error": "commit rev mismatch",
                "repo": repo, "seq": seq,
            }
        if root["did"] != decoded["repo"]:
            return {
                "ok": False, "fatal": True,
                "error": "commit DID mismatch",
                "repo": repo, "seq": seq,
            }

        # Verify signature
        to_verify = root.copy()
        to_verify_sig = to_verify.pop("sig")
        to_verify = cbrrr.encode_dag_cbor(to_verify)

        try:
            parse_pubkey(atproto_key).verify(to_verify_sig, to_verify)
        except InvalidSignature:
            return {"ok": False, "retry": True, "repo": repo, "seq": seq}

        # Process ops
        rows = []
        rev_int = int.from_bytes(sb32d(decoded["rev"]))
        since_int = (
            int.from_bytes(sb32d(decoded["since"]))
            if decoded["since"] is not None
            else None
        )

        for op in decoded["ops"]:
            op_cid = op.get("cid")
            res = mst.mst_dfs(pld, set(), root["data"], op["path"], op_cid)
            if not res[0]:
                return {
                    "ok": False,
                    "error": f"mst proof failed for {op['path']}",
                    "repo": repo, "seq": seq,
                }

            collection, rkey = op["path"].split("/")

            if op_cid is None:
                value_bytes = b"null"
            else:
                value_bytes = orjson.dumps(
                    cbrrr.decode_dag_cbor(
                        cbrrr.encode_dag_cbor(pld.get(op_cid)), atjson_mode=True
                    )
                )

            rows.append((
                root["did"],
                collection,
                rkey,
                rev_int,
                value_bytes,
                since_int,
                op["action"],
            ))

        return {"ok": True, "rows": rows, "repo": repo}

    except Exception as exc:
        # repo and seq are set early in the try block; they'll be None
        # only if the initial CBOR decode itself failed
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "repo": repo,
            "seq": seq,
        }
