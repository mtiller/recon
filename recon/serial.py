import bz2
from util import _read, _read_nolen

#        if self.compression:
#            c = bz2.BZ2Compressor()
#            a = c.compress(bdata)
#            b = c.flush()
#            bdata = a+b


class BSONSerializer(object):
    def __init__(self, compress=False, verbose=False):
        from bson import BSON

        self.bson = BSON()
        self.compress = compress
        self.verbose = verbose
    def encode(self, x):
        return self.bson.encode(x)
    def decode(self, fp, length=None):
        if length==None:
            return _read_nolen(fp, verbose=self.verbose)
        else:
            return _read(fp, length, verbose=self.verbose)

class MsgPackSerializer(object):
    import msgpack
    def __init__(self):
        pass
    def encode(self, x, verbose=False):
        return msgpack.packb(x)
    def decode(self, fp, length=None, verbose=False):
        return msgpack.unpack(fp)
