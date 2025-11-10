import cbrrr
from leb128 import u as leb

from io import BytesIO
from hashlib import sha256


def parse(car):
    car = BytesIO(car)

    len, _ = leb.decode_reader(car)
    header = cbrrr.decode_dag_cbor(car.read(len))

    out = {}

    while True:
        try:
            len, _ = leb.decode_reader(car)
        except EOFError:
            break

        data = car.read(len)
        cid = cbrrr.CID(data[:36])
        assert data[:4] == b"\x01q\x12 "
        assert sha256(data[36:]).digest() == data[4:36]
        data = cbrrr.decode_dag_cbor(data[36:])
        out[cid] = data

    return header, out