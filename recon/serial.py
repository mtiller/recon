import bz2

def compress(data):
    c = bz2.BZ2Compressor()
    a = c.compress(data)
    b = c.flush()
    return a+b

def decompress(data):
    c = bz2.BZ2Decompressor()
    return c.decompress(data)

class BSONSerializer(object):
    def __init__(self, compress=False, verbose=False):
        from bson import BSON

        self.bson = BSON()
        self.compress = compress
        self.verbose = verbose
    def encode(self, x, uncomp=False):
        data = self.bson.encode(x)
        if self.compress and not uncomp:
            data = compress(data)
        return data
    def decode(self, fp, length, uncomp=False):
        data = fp.read(length)
        if self.compress and not uncomp:
            data = compress(data)
        return BSON(data).decode()

class MsgPackSerializer(object):
    def __init__(self, compress=False):
        self.compress = compress
    def encode(self, x, verbose=False, uncomp=False):
        import msgpack
        data = msgpack.packb(x)
        if self.compress and not uncomp:
            data = compress(data)
        return data
    def decode(self, fp, length, verbose=False, uncomp=False):
        import msgpack
        data = fp.read(length)
        if self.compress and not uncomp:
            data = decompress(data)
        x = msgpack.unpackb(data)
        return x
