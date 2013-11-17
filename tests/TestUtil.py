from StringIO import StringIO
from recon.util import _read_nolen, _read_compressed
from nose.tools import *

def test1():
    b = StringIO()
    x = _read_nolen(b, True)
    assert x==None

@raises(IOError)
def test2():
    b = StringIO("\x00")
    x = _read_nolen(b, True)

@raises(IOError)
def test3():
    b = StringIO("\x00\x00")
    x = _read_nolen(b, True)

@raises(IOError)
def test4():
    b = StringIO("\x00\x00\x00")
    x = _read_nolen(b, True)

@raises(IOError)
def test5():
    b = StringIO("\x08\x00\x00\x00\x01\x00")
    x = _read_nolen(b, True)

@raises(IOError)
def test6():
    b = StringIO("\x08\x00\x00\x00\x01\x00")
    x = _read_compressed(b, 100, True)
