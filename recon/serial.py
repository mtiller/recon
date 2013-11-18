import bz2
from util import _read, _read_nolen, _read_compressed

class BSONSerializer(object):
    def __init__(self, compress=False, verbose=False):
        from bson import BSON

        self.bson = BSON()
        self.compress = compress
        self.verbose = verbose
    def encode(self, x, nocomp=False):
        data = self.bson.encode(x)
        if self.compress and not nocomp:
            c = bz2.BZ2Compressor()
            a = c.compress(data)
            b = c.flush()
            data = a+b
        return data
    def decode(self, fp, length=None):
        if length==None:
            data = _read_nolen(fp, verbose=self.verbose)
        else:
            if self.compress:
                data = _read_compressed(fp, length, verbose=self.verbose)
            else:
                data = _read(fp, length, verbose=self.verbose)
        return data

class MsgPackSerializer(object):
    def __init__(self, compress=False):
        self.compress = compress
    def encode(self, x, verbose=False, nocomp=False):
        import msgpack
        return msgpack.packb(x)
    def decode(self, fp, length=None, verbose=False):
        import msgpack
        return msgpack.unpack(fp)
