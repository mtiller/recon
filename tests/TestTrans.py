from recon.trans import wall2meld

from recon.wall import WallReader, WallWriter
from recon.meld import MeldReader, MeldWriter

from TestWall import write_wall

from nose.tools import *

def testWall2Meld():
    write_wall(verbose=True);

    with open("sample.wll", "rb") as wfp:
        with open("sample.mld", "w+") as mfp:
            wall2meld(wfp, mfp)

    with open("sample.mld", "rb") as fp:
        meld = MeldReader(fp, verbose=True)

        print "Objects:"
        for objname in meld.objects():
            obj = meld.read_object(objname)
            print "  "+objname+" = "+str(obj)

        print "Tables:"
        for tabname in meld.tables():
            table = meld.read_table(tabname)
            print "  "+tabname
            for signal in table.signals():
                print "    #"+signal+": "+str(table.data(signal))

        print "table.metadata = "+str(table.metadata)
        print "table.var_metadata = "+str(table.var_metadata)

        assert_equals(meld.metadata, {"a": "bar"})
        assert_equals(table.signals(), ["time", "x", "y", "a", "b"])
        assert_equals(table.data("time"), [0.0, 1.0, 2.0])
        assert_equals(table.metadata, {"b": "foo"})
        assert_equals(table.var_metadata["time"], {"units": "s"})
        assert_equals(table.data("x"), [1.0, 0.0, 1.0])
        assert_equals(table.data("y"), [2.0, 3.0, 3.0])
        assert_equals(table.data("a"), [2.0, 1.0, 2.0])
        assert_equals(table.data("b"), [-2.0, -3.0, -3.0])
