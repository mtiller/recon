import bz2

def compress(data):
    """
    Helper function to compress data (using bz2)
    """
    c = bz2.BZ2Compressor()
    a = c.compress(data)
    b = c.flush()
    return a+b

def decompress(data):
    """
    Helper function to decompress data (using bz2)
    """
    c = bz2.BZ2Decompressor()
    return c.decompress(data)

class BSONSerializer(object):
    """
    This class supports BSON serialization.  We started with this
    but found it very inefficient for arrays.  So we dropped it
    in favor of msgpack.

    ** Deprecated **

    """
    def __init__(self, compress=False, verbose=False, single=False):
        """
        Initialize settings for this serializer
        """
        from bson import BSON

        self.bson = BSON()
        self.compress = compress
        self.verbose = verbose
    def encode_obj(self, x, uncomp=False):
        """
        Encode an object (uncomp=True means suppress compression)
        """
        data = self.bson.encode(x)
        if self.compress and not uncomp:
            data = compress(data)
            y = decompress(data) # Just to make sure it works
        return data
    def encode_vec(self, x, uncomp=False):
        """
        Encode a vector (uncomp=True means suppress compression)
        """
        obj = {"d": x}
        data = self.encode_obj(obj, uncomp=uncomp)
        return data
    def decode_obj(self, fp, length, uncomp=False):
        """
        Decode an object (uncomp=True means suppress decompression)
        """
        from bson import BSON

        data = fp.read(length)
        if self.compress and not uncomp:
            data = decompress(data)
        return BSON(data).decode()
    def decode_vec(self, fp, length, uncomp=False):
        """
        Decode a vector (uncomp=True means suppress decompression)
        """
        d = self.decode_obj(fp, length=length, uncomp=uncomp)
        return d["d"]

class MsgPackSerializer(object):
    def __init__(self, compress=False, single=False):
        """
        Initialize various settings
        """
        self.compress = compress
        self.single = single
    def encode_obj(self, x, verbose=False, uncomp=False):
        """
        Encode an object (uncomp=True means suppress compression)
        """
        import msgpack
        try:
            data = msgpack.packb(x, use_single_float=self.single)
            if self.compress and not uncomp:
                data = compress(data)
            return data
        except Exception as e: # pragma: no cover
            print "Exception thrown while trying to pack '"+str(x)+"'"
            if type(x)==list:
                print "  List contains: "+str(type(x[0]))
            raise e
    def encode_vec(self, x, verbose=False, uncomp=False):
        """
        Encode a vector (uncomp=True means suppress compression)
        """
        return self.encode_obj(x, verbose=verbose, uncomp=uncomp)
    def decode_obj(self, fp, length, verbose=False, uncomp=False):
        """
        Decode an object (uncomp=True means suppress decompression)
        """
        import msgpack
        data = fp.read(length)
        if self.compress and not uncomp:
            data = decompress(data)
        x = msgpack.unpackb(data)
        return x
    def decode_vec(self, fp, length, verbose=False, uncomp=False):
        """
        Decode a vector (uncomp=True means suppress decompression)
        """
        return self.decode_obj(fp, length=length,
                               verbose=verbose, uncomp=uncomp)
