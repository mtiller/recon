from bson import BSON

def _read(fp, verbose):
    """
    This method reads the next "document" from the file.  Because we are using
    BSON underneath, this just reads a given BSON sequence of bytes and translates
    it into a Python dictionary.
    """
    # Read the least significant and most significant bytes that describe the
    # length of the BSON document.
    b1 = fp.read(1);
    if len(b1)==0:
        return None
    if verbose:
        print "B1: "+str(ord(b1))
    b2 = fp.read(1);
    if len(b2)==0:
        raise IOError("Premature EOF");
    if verbose:
        print "B1: "+str(ord(b2))
    b3 = fp.read(1);
    if len(b3)==0:
        raise IOError("Premature EOF");
    if verbose:
        print "B1: "+str(ord(b3))
    b4 = fp.read(1);
    if len(b4)==0:
        raise IOError("Premature EOF");
    if verbose:
        print "B1: "+str(ord(b4))
    # Compute the length of the BSON string
    l = (((ord(b4)*256)+ord(b3)*256)+ord(b2)*256)+ord(b1)
    if verbose:
        print "Len: "+str(l)
    # Read the BSON data
    data = fp.read(l-4);
    if verbose:
        print "Raw Data: "+str(repr(data))
    if len(data)<l-4:
        raise IOError("Premature EOF");
    # Concatenate all the bytes (length and data) into a valid BSON sequence,
    # decode it and return it.
    data = b1+b2+b3+b4+data
    return BSON(data).decode()

