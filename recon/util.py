from bson import BSON
import struct
import bz2

def write_len(fp, l):
    fp.write(struct.pack('<L', l))

def conv_len(bytes):
    up = struct.unpack('<L', bytes)
    return up[0]

def read_len(fp, ignoreEOF=False):
    lbytes = fp.read(4)
    if len(lbytes)!=4:
        if ignoreEOF:
            return None
        else:
            raise IOError("Failed to read length data")
    up = struct.unpack('<L', lbytes)
    return up[0]
