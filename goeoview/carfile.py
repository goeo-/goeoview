import cbrrr
from leb128 import u as leb

from io import BytesIO
from hashlib import sha256


class CARFileError(ValueError):
    pass


def parse(car):
    car = BytesIO(car)

    length, _ = leb.decode_reader(car)
    header = cbrrr.decode_dag_cbor(car.read(length))

    out = {}

    while True:
        try:
            length, _ = leb.decode_reader(car)
        except EOFError:
            break

        data = car.read(length)
        if len(data) < 36:
            raise CARFileError("truncated CAR block")
        cid = cbrrr.CID(data[:36])
        if data[:4] != b"\x01q\x12 ":
            raise CARFileError("unsupported CAR CID prefix")
        if sha256(data[36:]).digest() != data[4:36]:
            raise CARFileError("CAR block hash mismatch")
        data = cbrrr.decode_dag_cbor(data[36:])
        out[cid] = data

    return header, out
