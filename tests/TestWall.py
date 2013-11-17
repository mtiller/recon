from recon.wall import WallWriter, WallReader, FinalizedWall
from nose.tools import *

def write_wall(verbose=False):
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=verbose)

        # Walls can contain tables, here is how we define one
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);

        # Tables can also have aliases, here is how we define a few
        t.add_alias(alias="a", of="x", scale=1.0, offset=1.0);
        t.add_alias(alias="b", of="y", scale=-1.0, offset=0.0);

        # Walls can also have objects.
        obj1 = wall.add_object("obj1");
        obj2 = wall.add_object("obj2");

        # Note, so far we have not specified the values of anything.
        # Once we "define" the wall, we can't change the structure,
        # but we can add rows to tables and fields to objects.
        wall.finalize();

        t.add_row(time=0.0, x=1.0, y=2.0)
        wall.flush()  # We can write data out at any time
        t.add_row(time=1.0, x=0.0, y=3.0)
        wall.flush()
        t.add_row(2.0, 1.0, 3.0)
        wall.flush()

        # Here we are adding fields to our object
        obj1.add_field("name", "Mike");
        obj2.add_field("name", "Pete");
        wall.flush();

        # Adding more fields
        obj1.add_field("nationality", "American");
        obj2.add_field("nationality", "UKLander");
        wall.flush();

        # Question, should we be allowed to overwrite fields?
        # If we are really journaling, this should be ok, e.g.,
        obj2.add_field("nationality", "GreatBritisher");
        wall.flush();

def read_wall(verbose=False):
    with open("sample.wll", "rb") as fp:
        wall = WallReader(fp, verbose=verbose)

        print "Objects:"
        for objname in wall.objects():
            obj = wall.read_object(objname)
            print "  "+objname+" = "+str(obj)

        print "Tables:"
        for tabname in wall.tables():
            table = wall.read_table(tabname)
            print "  "+tabname
            for signal in table.signals():
                print "    #"+signal+": "+str(table.data(signal))
            for alias in table.aliases():
                print "    @"+alias+": "+str(table.data(alias))

def testValidFile():
    write_wall()
    read_wall()

@raises(KeyError)
def testDuplicate1():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);

@raises(KeyError)
def testDuplicate2():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);
        t = wall.add_object(name="T1")

@raises(KeyError)
def testDuplicate3():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_object(name="T1")
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);

@raises(KeyError)
def testDuplicate4():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_object(name="T1")
        t = wall.add_object(name="T1")

@raises(FinalizedWall)
def testDuplicate5():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_object(name="T1")
        wall.finalize()
        t = wall.add_object(name="T2")

@raises(FinalizedWall)
def testDuplicate6():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);
        wall.finalize()
        t = wall.add_table(name="T2", signals=["time", "x", "y"]);

@raises(KeyError)
def testDuplicate7():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);
        t.add_alias("time", of="x")
        wall.finalize()

def testEmpty():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);
        wall.finalize()

    with open("sample.wll", "rb") as fp:
        wall = WallReader(fp)
        t = wall.read_table("T1")
        t.data("x")

@raises(NameError)
def testMissingSignal():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);
        wall.finalize()
        t.add_row(time=0.0, x=1.0, y=2.0)

    with open("sample.wll", "rb") as fp:
        wall = WallReader(fp)
        t = wall.read_table("T1")
        t.data("z")

@raises(ValueError)
def testBadArgs():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);
        wall.finalize()
        t.add_row(time=0.0, x=1.0, y=2.0)
        t.add_row(0.0, 1.0, 2.0)
        t.add_row(0.0, 1.0, y=2.0)

def testMetadata1():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        wall.metadata["a"] = "bar"
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);
        t.metadata["b"] = "foo"
        t.set_var_metadata("time", units="s")
        wall.finalize()
        t.add_row(time=0.0, x=1.0, y=2.0)

    with open("sample.wll", "rb") as fp:
        wall = WallReader(fp)
        assert wall.metadata=={"a": "bar"}
        t = wall.read_table("T1")
        assert t.metadata=={"b": "foo"}

@raises(NameError)
def testMetadata2():
    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        wall.metadata["a"] = "bar"
        t = wall.add_table(name="T1", signals=["time", "x", "y"]);
        t.metadata["b"] = "foo"
        t.set_var_metadata("z", units="s")
        wall.finalize()
        t.add_row(time=0.0, x=1.0, y=2.0)

