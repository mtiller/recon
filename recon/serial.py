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
    def encode_obj(self, x, uncomp=False):
        data = self.bson.encode(x)
        if self.compress and not uncomp:
            data = compress(data)
            y = decompress(data) # Just to make sure it works
        return data
    def encode_vec(self, x, uncomp=False):
        obj = {"d": x}
        data = self.encode_obj(obj, uncomp=uncomp)
        return data
    def decode_obj(self, fp, length, uncomp=False):
        from bson import BSON

        data = fp.read(length)
        if self.compress and not uncomp:
            data = decompress(data)
        return BSON(data).decode()
    def decode_vec(self, fp, length, uncomp=False):
        d = self.decode_obj(fp, length=length, uncomp=uncomp)
        return d["d"]

class MsgPackSerializer(object):
    def __init__(self, compress=False):
        self.compress = compress
    def encode_obj(self, x, verbose=False, uncomp=False):
        import msgpack
        try:
            data = msgpack.packb(x)
            if self.compress and not uncomp:
                data = compress(data)
            return data
        except Exception as e:
            print "Exception thrown while trying to pack '"+str(x)+"'"
            if type(x)==list:
                print "  List contains: "+str(type(x[0]))
            raise e
    def encode_vec(self, x, verbose=False, uncomp=False):
        return self.encode_obj(x, verbose=verbose, uncomp=uncomp)
    def decode_obj(self, fp, length, verbose=False, uncomp=False):
        import msgpack
        data = fp.read(length)
        if self.compress and not uncomp:
            data = decompress(data)
        x = msgpack.unpackb(data)
        return x
    def decode_vec(self, fp, length, verbose=False, uncomp=False):
        return self.decode_obj(fp, length=length,
                               verbose=verbose, uncomp=uncomp)
