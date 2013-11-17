from nose.tools import *
from recon.meld import FinalizedMeld, MissingData, WriteAfterClose
from recon.meld import MeldWriter, MeldReader

def write_meld(compression=False, verbose=False,n=0,name="sample"):
    with open(name+".mld", "w+") as fp:
        meld = MeldWriter(fp, verbose=verbose, compression=compression)
        
        # Need to identify all entities in the file first.  We don't need
        # their data.  We just need to enumerate them for the header.
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        t.add_alias(alias="a", of="x", scale=1.0, offset=1.0);
        t.add_alias(alias="b", of="y", scale=-1.0, offset=0.0);
        obj1 = meld.add_object("obj1");
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
        meld.close()

def read_meld(verbose=True, name="sample"):
    with open(name+".mld", "rb") as fp:
        meld = MeldReader(fp, verbose=verbose)

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

def testUncompressedValidMeld1():
    write_meld(verbose=False,compression=False,n=100)
    read_meld(verbose=False)

def testUncompressedValidMeld2():
    write_meld(verbose=True,compression=False,n=100)
    read_meld(verbose=True)

def testCompressedValidMeld1():
    write_meld(verbose=True,compression=True,name="sample_comp",n=100)
    read_meld(verbose=True, name="sample_comp")

def testCompressedValidMeld2():
    write_meld(verbose=True,compression=True,name="sample_comp",n=100)
    read_meld(verbose=True)

@raises(NameError)
def testDuplicateTable1():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);

@raises(NameError)
def testDuplicateTable2():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        t = meld.add_object(name="T1")

@raises(FinalizedMeld)
def testDuplicateTable3():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        meld.finalize()
        t = meld.add_table(name="T2", signals=["time", "x", "y"]);

@raises(NameError)
def testDuplicateTable4():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        t.write("z", [2.0, 3.0, 3.0]);

@raises(MissingData)
def testDuplicateTable5():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        meld.close()

@raises(NameError)
def testDuplicateTable6():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        t.add_alias("x", of="y")
        meld.close()

@raises(NameError)
def testDuplicateTable7():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
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

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        t.add_alias("z", of="a")
        meld.close()

@raises(ValueError)
def testDuplicateTable9():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        meld.finalize()
        t.write("time", 2.0);
        meld.close()

@raises(ValueError)
def testDuplicateTable10():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        meld.finalize()
        t.write("time", 2.0);
        meld.close()
        t.write("time", 2.0);

@raises(WriteAfterClose)
def testDuplicateTable11():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        meld.finalize()
        t.write("time", [2.0, 1.0, 0.0]);
        t.write("time", [2.0, 1.0, 0.0]);
        meld.close()

@raises(NameError)
def testDuplicateObject1():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_object(name="T1")
        t = meld.add_object(name="T1")

@raises(NameError)
def testDuplicateObject2():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_object(name="T1")
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);

@raises(FinalizedMeld)
def testDuplicateObject3():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_object(name="T1")
        meld.finalize()
        t = meld.add_object(name="T2")

def testDuplicateObject4():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_object(name="T1")
        meld.finalize()
        t.write(x=2.0,y=3.0)
        meld.close()

@raises(WriteAfterClose)
def testDuplicateObject5():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_object(name="T1")
        meld.finalize()
        t.write(x=2.0,y=3.0)
        meld.close()
        t.write(x=2.0,y=3.0)

@raises(NameError)
def testNoSuchSignal():
    from recon.meld import MeldWriter

    with open("sample_test.mld", "w+") as fp:
        meld = MeldWriter(fp)
        t = meld.add_table(name="T1", signals=["time", "x", "y"]);
        meld.finalize()
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);
        meld.close()

    with open("sample_test.mld", "rb") as fp:
        meld = MeldReader(fp, verbose=True)
        t = meld.read_table("T1")
        x = t.data("x")
        a = t.data("a")
