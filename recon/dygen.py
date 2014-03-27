import struct
import time

class DymolaResultsGenerator(object):
    """
    A Python generator that returns the contents of a
    Dymola results file as a *generator*.  This allows
    us to parse it chuck by chunk instead of reading the
    entire thing into memory!
    """
    def __init__(self, f, e=None, poll=0.1):
        if type(f)==str:
            self.file = open(f, "rb")
            self.shouldClose = True
        else:
            # 'f' is a "file-like" object
            self.file = f
            self.shouldClose = False

        self.event = e
        self.poll = poll;
        self.endian = None;
        self.o = None;
        self.dtype = None;
        self.dsize = None;
        self.text = None;
        self.mrows = None;
        self.ncols = None;
        self.imaginary = None;
        self.name = None;
    def _read(self, size):
        n = 0
        left = size-n
        buf = ""
        while n<size:
            s = self.file.read(left)
            buf += s
            n += len(s)
            left = size-n
            if self.event!=None and self.event.isSet():
                return None
            if self.poll!=None and n<size:
                time.sleep(self.poll)
        return buf
    def _parseHeader(self):
        # Read MOPT integer
        mopt = struct.unpack('i4', self._read(4))[0]
        m = mopt/1000;
        if m==0:
            self.endian = "<"
        elif m==1:
            self.endian = ">"
        else:
            raise ValueError("M values other than 0 or 1 not supported")
        mopt -= m*1000;
        self.o = mopt/100;
        mopt -= self.o*100;

        p = mopt/10;
        if p==0:
           self.dtype = 'd'
           self.dsize = 8
        elif p==1:
            self.dtype = 'f'
            self.dsize = 4
        elif p==2:
            self.dtype = 'i'
            self.dsize = 4
        elif p==3:
            self.dtype = 'h'
            self.dsize = 2
        elif p==4:
            self.dtype = 'H'
            self.dsize = 2
        elif p==5:
            self.dtype = 'b'
            self.dsize = 1
        else:
            raise ValueError("Unknown data type, P="+str(p))

        mopt -= p*10;
        if mopt==0:
            self.text = False;
        elif mopt==1:
            self.text = True;
        else:
            raise ValueError("Sparse matrices are not supported")
        # Number of rows
        self.mrows = struct.unpack('i4', self._read(4))[0]
        # Number of columns
        self.ncols = struct.unpack('i4', self._read(4))[0]
        # Imaginary
        self.imaginary = struct.unpack('i4', self._read(4))[0]!=0
        if self.imaginary:
            raise ValueError("Imaginary matrices not supported")
        # Name
        nlen = struct.unpack('i4', self._read(4))[0]
        self.name = "".join(struct.unpack('c'*nlen, self._read(nlen))[:nlen-1])
        return None
    def _parseCol(self):
        size = self.dsize*self.mrows
        if self.text:
            dtype = self.endian+('c'*self.mrows)
        else:
            dtype = self.endian+(self.dtype*self.mrows)

        #dtype = self.dtype*self.ncols
        data = self._read(size)
        if data==None:
            return None
        row = struct.unpack(dtype, data)
        return row
    def read(self, unlimited=False):
        self._parseHeader()
        yield (self.name, self.mrows, self.ncols)
        # If the unlimited flag is set, we ignore the number
        # of rows and keep attempting to read until the event
        # fires.  I do this because the 'last' matrix in the
        # results file apparently has the size of its results
        # constantly updated.  So the initial number of rows
        # read is not correct.
        if unlimited:
            while not self.event.isSet():
                ret = self._parseCol()
                if ret==None:
                    if self.shouldClose:
                        self.file.close()
                    raise StopIteration
                else:
                    yield ret
        else:
            for i in xrange(0,self.ncols):
                yield self._parseCol()

def parseTransposeNames(g):
    ret = None
    for next in g:
        if ret==None:
            ret = ['']*len(next)
        for j in xrange(0, len(next)):
            ret[j] = ret[j] + next[j]
    for j in xrange(0, len(ret)):
        ret[j] = ret[j].strip().replace("\x00","")
    return ret

def parseNormalNames(g):
    ret = []
    for next in g:
        ret.append("".join(next).strip().replace("\x00",""))
    return ret

def parseNormalMatrix(g):
    ret = []
    for next in g:
        ret.append(list(next))
    return ret
