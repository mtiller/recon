class Transform(object):
    pass

class UnknownTransform(Exception):
    """Raised when an unrecognized exception is encountered"""

class Affine(Transform):
    def __init__(self, s, o):
        self.scale = s;
        self.offset = o;
    def encode(self):
        return {"k": "aff", "s": self.scale, "o": self.offset}
    def apply(self, v):
        if type(v)==float or type(v)==int or type(v)==long:
            return self.scale*v+self.offset;
        raise ValueError("Unable to apply affine transformation to "+str(type(v))+" data")

class Inverse(Transform):
    def __init__(self):
        pass;
    def encode(self):
        return {"k": "inv"}
    def apply(self, v):
        if type(v)==bool:
            return not v
        if type(v)==float or type(v)==int or type(v)==long:
            return -v
        raise ValueError("Unable to apply inverse transformation to "+
                         str(type(v))+" data") # pragma: nocover

def decode_transform(obj):
    if not "k" in obj:
        raise ValueError("Object '"+str(obj)+"' is not a transformation") # pragma: nocover
    kind = obj["k"]
    if kind=="aff":
        return Affine(obj["s"],obj["o"]);
    elif kind=="inv":
        return Inverse()
    else:
        raise UnknownTransform("Unknown kind of transformation: "+kind) # pragma: nocover
