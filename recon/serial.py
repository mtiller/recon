import bz2
from util import _read, _read_nolen, _read_compressed

class BSONSerializer(object):
    def __init__(self, compress=False, verbose=False):
        from bson import BSON

        self.bson = BSON()
        self.compress = compress
        self.verbose = verbose
    def encode(self, x, uncomp=False):
        data = self.bson.encode(x)
        if self.compress and not uncomp:
            c = bz2.BZ2Compressor()
            a = c.compress(data)
            b = c.flush()
            data = a+b
        return data
    def decode(self, fp, length):
        if self.compress:
            data = _read_compressed(fp, length, verbose=self.verbose)
        else:
            data = _read(fp, length, verbose=self.verbose)
        return data

class MsgPackSerializer(object):
    def __init__(self, compress=False):
        self.compress = compress
    def encode(self, x, verbose=False, uncomp=False):
        import msgpack
        data = msgpack.packb(x)
        print "SS "+str(x)+"("+str(len(data))+") => "+str(repr(data))
        return data
    def decode(self, fp, length, verbose=False):
        import msgpack
        print "Reading @"+str(fp.tell())
        bytes = fp.read(length)
        print "SS "+str(repr(bytes))+"("+str(len(bytes))+") => ?"
        data = msgpack.unpackb(bytes)
        print "SS     => "+str(data)
        return data
