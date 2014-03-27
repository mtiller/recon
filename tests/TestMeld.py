from nose.tools import *
from recon.meld import FinalizedMeld, MissingData, WriteAfterClose
from recon.meld import MeldWriter, MeldReader

from recon.transforms import Transform, Affine, Inverse

import os

def write_meld(name,compression=False,verbose=False,n=0):
    mfile = os.path.join("test_output",name+".mld")
    with MeldWriter(mfile, metadata={"source": "x"},
                    verbose=verbose, compression=compression) as meld:
        # Need to identify all entities in the file first.  We don't need
        # their data.  We just need to enumerate them for the header.
        t = meld.add_table(name="T1", metadata={"model": "Foo"})
        t.add_signal("time", metadata={"units": "s"}, vtype=float)
        t.add_signal("x")
        t.add_signal("y")
        t.add_alias(alias="a", of="x", transform=Affine(1,1), metadata={"ax": "zed"});
        t.add_alias(alias="b", of="y", transform=Inverse());
        obj1 = meld.add_object("obj1", metadata={"a": "bar"});
        obj2 = meld.add_object("obj2");

        # All the structural information has been specified (perhaps this
        # could be implicitly invoked?)
        meld.finalize()

        # Now we can start writing the actual data to the file.  As soon as we
        # start writing data, we can't made any changes that would affect the header
        t.write("time", [0.0, 1.0, 2.0]+[0.0]*n);
        t.write("x", [1.0, 0.0, 1.0]+[0.0]*n);
        t.write("y", [2.0, 3.0, 3.0]+[0.0]*n);

        # Once we switch to writing another entity, the previous entity is
        # implicitly finalized
        obj1.write(nationality="American", name="Mike");

        obj2.write(nationality="GreatBritisher", name="Pete");

        # When the meld is closed (and it must be closed!), the entity that was
        # currently being written is finalized
        #meld.close()

def read_meld(name,verbose=True):
    with MeldReader(os.path.join("test_output",name+".mld"), verbose=verbose) as meld:
        assert_equals(meld.metadata, {"source": "x"})

        print "Objects:"
        for objname in meld.objects():
            obj = meld.read_object(objname)
            print "  "+objname+" = "+str(obj)
            if objname=="obj1":
                assert_equals(obj.metadata, {"a": "bar"})

        print "Tables:"
        for tabname in meld.tables():
            table = meld.read_table(tabname)
            print "  "+tabname
            for signal in table.signals():
                print "    #"+signal+": "+str(table.data(signal))
            assert_equals(table.metadata["model"], "Foo")
            assert_equals(table.var_metadata["a"], {"ax": "zed"})
            assert_equals(table.var_metadata["time"], {"units": "s"})

def testUncompressedValidMeld1():
    write_meld(name="sample_ucmeld1",verbose=False,compression=False,n=100)
    read_meld(name="sample_ucmeld1",verbose=False)

def testUncompressedValidMeld2():
    write_meld(name="sample_ucmeld2",verbose=True,compression=False,n=100)
    read_meld(name="sample_ucmeld2",verbose=True)

def testCompressedValidMeld1():
    write_meld(name="sample_cmeld1",verbose=True,compression=True,n=100)
    read_meld(name="sample_cmeld1",verbose=True)

def testCompressedValidMeld2():
    write_meld(name="sample_cmeld2",verbose=True,compression=True,n=100)
    read_meld(name="sample_cmeld2",verbose=True)

@raises(NameError)
def testDuplicateTable1():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab1.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        t = meld.add_table(name="T1")
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")

@raises(NameError)
def testDuplicateTable2():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab2.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        t = meld.add_object(name="T1")

@raises(FinalizedMeld)
def testDuplicateTable3():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab3.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t = meld.add_table(name="T2")
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")

@raises(FinalizedMeld)
def testDuplicateTable3b():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab3b.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        meld.finalize()
        t.add_signal("y")

@raises(NameError)
def testDuplicateTable4():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab4.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        t.write("z", [2.0, 3.0, 3.0]);

@raises(MissingData)
def testDuplicateTable5():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab5.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        meld.close()

@raises(NameError)
def testDuplicateTable6():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab6.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1")
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        t.add_alias("x", of="y")
        meld.close()

@raises(NameError)
def testDuplicateTable7():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab7.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        t.add_alias("z", of="y")
        t.add_alias("z", of="x")
        meld.close()

@raises(NameError)
def testDuplicateTable8():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab8.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        t.add_alias("z", of="a")
        meld.close()

@raises(ValueError)
def testDuplicateTable9():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab9.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", 2.0);
        meld.close()

@raises(ValueError)
def testDuplicateTable10():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab10.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", 2.0);
        meld.close()
        t.write("time", 2.0);

@raises(WriteAfterClose)
def testDuplicateTable11():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dtab11.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1")
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", [2.0, 1.0, 0.0]);
        t.write("time", [2.0, 1.0, 0.0]);
        meld.close()

@raises(NameError)
def testDuplicateObject1():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dobj1.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_object(name="T1")
        t = meld.add_object(name="T1")

@raises(NameError)
def testDuplicateObject2():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dobj2.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_object(name="T1")
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")

@raises(FinalizedMeld)
def testDuplicateObject3():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dobj3.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_object(name="T1")
        meld.finalize()
        t = meld.add_object(name="T2")

def testDuplicateObject4():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dobj4.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_object(name="T1")
        meld.finalize()
        t.write(x=2.0,y=3.0)
        meld.close()

@raises(WriteAfterClose)
def testDuplicateObject5():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_dobj5.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_object(name="T1")
        meld.finalize()
        t.write(x=2.0,y=3.0)
        meld.close()
        t.write(x=2.0,y=3.0)

@raises(NameError)
def testNoSuchTable():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_ntab.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        meld.close()

    with open(os.path.join("test_output","sample_ntab.mld"), "rb") as fp:
        meld = MeldReader(fp, verbose=True)
        t = meld.read_table("T2")

@raises(NameError)
def testNoSuchSignal():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_nsig.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        meld.close()

    with open(os.path.join("test_output","sample_nsig.mld"), "rb") as fp:
        meld = MeldReader(fp, verbose=True)
        t = meld.read_table("T1")
        x = t.data("x")
        a = t.data("a")

@raises(NameError)
def testNoSuchObject():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_nobj.mld"), "wb+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        meld.close()

    with open(os.path.join("test_output","sample_nobj.mld"), "rb") as fp:
        meld = MeldReader(fp, verbose=True)
        t = meld.read_object("T1")

def testMetadata1():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_md1.mld"), "wb+") as fp:
        meld = MeldWriter(fp, metadata={"a": "bar"})
        t = meld.add_table(name="T1", metadata={"b": "foo"});
        t.add_signal("time", metadata={"units": "s"})
        t.add_signal("x")
        t.add_signal("y")
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        meld.close()

    with open(os.path.join("test_output","sample_md1.mld"), "rb") as fp:
        meld = MeldReader(fp, verbose=True)
        assert meld.metadata == {"a": "bar"}
        t = meld.read_table("T1")
        assert t.metadata == {"b": "foo"}
        assert t.var_metadata["time"]=={"units": "s"}

@raises(TypeError)
def testNotAType():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_ntype.mld"), "wb+") as fp:
        meld = MeldWriter(fp, metadata={"a": "bar"})
        t = meld.add_table(name="T1", metadata={"b": "foo"});
        t.add_signal("time", metadata={"units": "s"}, vtype="float")
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        meld.close()

@raises(TypeError)
def testTypeMismatch1():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_tmis1.mld"), "wb+") as fp:
        meld = MeldWriter(fp, metadata={"a": "bar"})
        t = meld.add_table(name="T1", metadata={"b": "foo"});
        t.add_signal("time", metadata={"units": "s"}, vtype=float)
        meld.finalize()
        t.write("time", [0.0, 1.0, 2]);
        meld.close()

@raises(TypeError)
def testTypeMismatch2():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_tmis2.mld"), "wb+") as fp:
        meld = MeldWriter(fp, metadata={"a": "bar"})
        t = meld.add_table(name="T1", metadata={"b": "foo"});
        t.add_signal("time", metadata={"units": "s"}, vtype=int)
        meld.finalize()
        t.write("time", [0.0, 1.0, 2]);
        meld.close()

@raises(TypeError)
def testTypeMismatch3():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_tmis3.mld"), "wb+") as fp:
        meld = MeldWriter(fp, metadata={"a": "bar"})
        t = meld.add_table(name="T1", metadata={"b": "foo"});
        t.add_signal("time", metadata={"units": "s"}, vtype=bool)
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        meld.close()

def testChecking1():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_c1.mld"), "wb+") as fp:
        meld = MeldWriter(fp, metadata={"a": "bar"})
        t = meld.add_table(name="T1", metadata={"b": "foo"});
        t.add_signal("time", metadata={"units": "s"}, vtype=bool)
        meld.finalize()
        t.write("time", [True, False, True]);
        meld.close()

def testChecking2():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_c2.mld"), "wb+") as fp:
        meld = MeldWriter(fp, metadata={"a": "bar"})
        t = meld.add_table(name="T1", metadata={"b": "foo"});
        t.add_signal("time", metadata={"units": "s"}, vtype=int)
        meld.finalize()
        t.write("time", [0, 1, 2]);
        meld.close()

def testChecking3():
    from recon.meld import MeldWriter

    with open(os.path.join("test_output","sample_c3.mld"), "wb+") as fp:
        meld = MeldWriter(fp, metadata={"a": "bar"})
        t = meld.add_table(name="T1", metadata={"b": "foo"});
        t.add_signal("time", metadata={"units": "s"}, vtype=str)
        meld.finalize()
        t.write("time", ["this", "is", "a", "test"]);
        meld.close()
