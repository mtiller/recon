import struct
import bz2

def write_len(fp, l):
    """
    A frequently used utility function to write an integer
    to a given stream.
    """
    fp.write(struct.pack('!L', l))

def conv_len(bytes):
    """
    This takes some bytes and converts them to an integer following
    the same conventions used by the other routines in this file.
    """
    up = struct.unpack('!L', bytes)
    return up[0]

def read_len(fp, ignoreEOF=False, verbose=False):
    """
    This reads a length from the stream.  If the ignoreEOF flag
    is set, a failure to read the length simple results in
    a None being returned (vs. an exception being thrown)
    """
    lbytes = fp.read(4)
    #if verbose:
    #    print "Raw length bytes: "+str(repr(lbytes))
    if len(lbytes)!=4:
        if ignoreEOF:
            return None
        else: # pragma no cover
            raise IOError("Failed to read length data")
    up = struct.unpack('!L', lbytes)
    return up[0]

# Transforms

T_INV = "inv"
T_AFF = "aff"

class InvTransform:
    def __init__(self):
        pass
    def apply(self, data):
        def afunc(x):
            if type(x)==bool:
                return not x
            if type(x)==float:
                return -x
            if type(x)==int: # pragma: no cover
                return -x
            if type(x)==long: # pragma: no cover
                return -x
            else: # pragma: no cover
                return x
        return map(lambda x: afunc(x), data)

class AffineTransform:
    def __init__(self, scale, offset):
        self.scale = scale
        self.offset = offset
    def apply(self, data):
        def sfunc(x):
            # TODO: Are these sufficient?
            if type(x)==float or type(x)==int or type(x)==long:
                return x*self.scale+self.offset
            else: # pragma: no cover
                return x
        return map(lambda x: sfunc(x), data)

def parse_transform(t):
    if t==None:
        return None
    if type(t)!=str:
        return None

    trans = t.replace(" ","")

    if trans==T_INV:
        return InvTransform()
    if trans.startswith(T_AFF+"(") and trans.endswith(")"):
        try:
            (s, o) = map(lambda x: float(x), trans[4:-1].split(","))
            return AffineTransform(s, o)
        except:
            return None
