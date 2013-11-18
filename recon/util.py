from bson import BSON
import struct
import bz2

def write_len(fp, l):
    """
    A frequently used utility function to write an integer
    to a given stream.
    """
    fp.write(struct.pack('<L', l))

def conv_len(bytes):
    """
    This takes some bytes and converts them to an integer following
    the same conventions used by the other routines in this file.
    """
    up = struct.unpack('<L', bytes)
    return up[0]

def read_len(fp, ignoreEOF=False):
    """
    This reads a length from the stream.  If the ignoreEOF flag
    is set, a failure to read the length simple results in
    a None being returned (vs. an exception being thrown)
    """
    lbytes = fp.read(4)
    if len(lbytes)!=4:
        if ignoreEOF:
            return None
        else:
            raise IOError("Failed to read length data")
    up = struct.unpack('<L', lbytes)
    return up[0]
