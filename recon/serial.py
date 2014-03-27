import bz2
import msgpack

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

class BSONSerializer(object): # pragma: no cover
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
    def decode_obj(self, fp, length, uncomp=False, verbose=False):
        """
        Decode an object (uncomp=True means suppress decompression)
        """
        from bson import BSON

        data = fp.read(length)
        if self.compress and not uncomp:
            data = decompress(data)
        if verbose:
            print "Raw object data: "+str(repr(data))
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
    def encode_len(self, fp, l):
        return msgpack.pack(l, fp)
    def encode_obj(self, x, verbose=False, uncomp=False):
        """
        Encode an object (uncomp=True means suppress compression)
        """
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
    def decode_len(self, fp, ignoreEOF=False, verbose=False):
        # Assume the worst case and that the int is 9 bytes long
        bytes = fp.read(9)
        print "Bytes read: "+repr(bytes)
        if len(bytes)==0 and ignoreEOF:
            return None
        needed = None
        first = ord(bytes[0])
        print "first = %x" % (first)
        # If leading bit is zero, then the first byte is our int
        if (first & 0x80)==0:
            needed = 1
        elif first==0xcc or first==0xd0:
            needed = 2
        elif first==0xcd or first==0xd1:
            needed = 3
        elif first==0xce or first==0xd2:
            needed = 5
        elif first==0xcf or first==0xd3:
            needed = 9
        else:
            raise ValueError("Cannot determine number of bytes for integer")
        if len(bytes)<needed:
            raise ValueError("Insufficient bytes to decode integer")
        print "Number of bytes needed: "+str(needed)
        ret = msgpack.unpackb(bytes[0:needed])
        fp.seek(-len(bytes),1) # Rewind to where we were
        fp.seek(needed,1) # Move past bytes used to encode length
        return ret
    def decode_obj(self, fp, length, verbose=False, uncomp=False):
        """
        Decode an object (uncomp=True means suppress decompression)
        """
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

class UMsgPackSerializer(object):
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
        import umsgpack
        if self.single:
            umsgpack._float_size=32
        else:
            umsgpack._float_size=64
        print "umsgpack._float_size = "+str(umsgpack._float_size)
        
        try:
            data = umsgpack.packb(x)
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
        import umsgpack
        if self.single:
            umsgpack._float_size=32
        else:
            umsgpack._float_size=64

        data = fp.read(length)
        if self.compress and not uncomp:
            data = decompress(data)
        x = umsgpack.unpackb(data)
        return x
    def decode_vec(self, fp, length, verbose=False, uncomp=False):
        """
        Decode a vector (uncomp=True means suppress decompression)
        """
        return self.decode_obj(fp, length=length,
                               verbose=verbose, uncomp=uncomp)
