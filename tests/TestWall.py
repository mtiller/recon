from recon.wall import WallWriter, WallReader, FinalizedWall, NotFinalized
from nose.tools import *
import os

def write_wall(verbose=False):
    with open(os.path.join("test_output","sample.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, metadata={"a": "bar"}, verbose=verbose)

        # Walls can contain tables, here is how we define one
        t = wall.add_table(name="T1", metadata={"model": "Foo"});
        t.add_signal("time", metadata={"units": "s"}, vtype=float)
        t.add_signal("x", vtype=int)
        t.add_signal("y", vtype=str)
        t.add_signal("active")

        # Tables can also have aliases, here is how we define a few
        t.add_alias(alias="a", of="x", transform="aff(-1.0,2.0)", metadata={"ax": "zed"});
        t.add_alias(alias="b", of="y");
        t.add_alias(alias="inactive", of="active", transform="inv");

        # Walls can also have objects.
        obj1 = wall.add_object("obj1", metadata={"xyz": "ABC"});
        obj2 = wall.add_object("obj2");

        # Note, so far we have not specified the values of anything.
        # Once we "define" the wall, we can't change the structure,
        # but we can add rows to tables and fields to objects.
        wall.finalize();

        t.add_row(time=0.0, x=1, y="2.0", active=True)
        wall.flush()  # We can write data out at any time
        t.add_row(time=1.0, x=0, y="3.0", active=False)
        wall.flush()
        t.add_row(2.0, 1, "3.0", True)
        wall.flush()

        # Here we are adding fields to our object
        obj1.add_fields(name="Mike");
        wall.flush();

        # Adding more fields
        obj1.add_fields(nationality="American");
        obj2.add_fields(name="Pete", nationality="UKLander");
        wall.flush();

        # Question, should we be allowed to overwrite fields?
        # If we are really journaling, this should be ok, e.g.,
        obj2.add_fields(nationality="GreatBritisher");
        wall.flush();

def read_wall(verbose=False):
    with open(os.path.join("test_output","sample.wll"), "rb") as fp:
        wall = WallReader(fp, verbose=verbose)

        print "Objects:"
        for objname in wall.objects():
            obj = wall.read_object(objname)
            print "  "+objname+" = "+str(obj)
            if objname=="obj1":
                assert_equals(obj.metadata, {"xyz": "ABC"})

        print "Tables:"
        for tabname in wall.tables():
            table = wall.read_table(tabname)
            assert_equals(table.metadata, {"model": "Foo"})
            print "  "+tabname

            vs = table.variables() # For coverage

            for signal in table.signals():
                print "    #"+signal+": "+str(table.data(signal))
                if signal=="time":
                    assert_equals(table.var_metadata[signal], {"units": "s"})
            for alias in table.aliases():
                print "    @"+alias+": "+str(table.data(alias))
                if alias=="a":
                    assert_equals(table.var_metadata[alias], {"ax": "zed"})

        assert_equals(table.signals(),["time", "x", "y", "active"])
        assert_equals(table.data("time"),[0.0, 1.0, 2.0])
        assert_equals(table.data("x"),[1, 0, 1])
        assert_equals(table.data("y"),["2.0", "3.0", "3.0"])
        assert_equals(table.data("active"),[True, False, True])
        assert_equals(table.data("a"),[1.0, 2.0, 1.0])
        assert_equals(table.data("b"),["2.0", "3.0", "3.0"])
        assert_equals(table.data("inactive"),[False, True, False])

def testValidFile():
    write_wall()
    read_wall()

@raises(KeyError)
def testDuplicate1():
    with open(os.path.join("test_output","sample_1.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        t = wall.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")

@raises(KeyError)
def testDuplicate2():
    with open(os.path.join("test_output","sample_2.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        t = wall.add_object(name="T1")

@raises(KeyError)
def testDuplicate3():
    with open(os.path.join("test_output","sample_3.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_object(name="T1")
        t = wall.add_table(name="T1")
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")

@raises(KeyError)
def testDuplicate4():
    with open(os.path.join("test_output","sample_4.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_object(name="T1")
        t = wall.add_object(name="T1")

@raises(FinalizedWall)
def testDuplicate5():
    with open(os.path.join("test_output","sample_5.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_object(name="T1")
        wall.finalize()
        t = wall.add_object(name="T2")

@raises(FinalizedWall)
def testDuplicate6():
    with open(os.path.join("test_output","sample_6.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")

        wall.finalize()
        t = wall.add_table(name="T2");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")


@raises(KeyError)
def testDuplicate7():
    with open(os.path.join("test_output","sample_7.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)

        # Walls can contain tables, here is how we define one
        t = wall.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")

        t.add_alias("time", of="x")
        wall.finalize()

def testEmpty():
    with open(os.path.join("test_output","sample_8.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1");
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        wall.finalize()

    with open(os.path.join("test_output","sample.wll"), "rb") as fp:
        wall = WallReader(fp)
        t = wall.read_table("T1")
        t.data("x")

@raises(NameError)
def testMissingSignal():
    with open(os.path.join("test_output","sample_9.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1")
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        wall.finalize()
        t.add_row(time=0.0, x=1.0, y=2.0)

    with open(os.path.join("test_output","sample_9.wll"), "rb") as fp:
        wall = WallReader(fp)
        t = wall.read_table("T1")
        t.data("z")

@raises(ValueError)
def testBadArgs():
    with open(os.path.join("test_output","sample_10.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1")
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        wall.finalize()
        t.add_row(time=0.0, x=1.0, y=2.0)
        t.add_row(0.0, 1.0, 2.0)
        t.add_row(0.0, 1.0, y=2.0)

@raises(NotFinalized)
def testNotFinalRow():
    with open(os.path.join("test_output","sample_11.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1")
        t.add_signal("time")
        t.add_signal("x")
        t.add_signal("y")
        t.add_row(time=0.0, x=1.0, y=2.0)
        t.add_row(0.0, 1.0, 2.0)
        t.add_row(0.0, 1.0, y=2.0)

@raises(TypeError)
def testNotAType():
    with open(os.path.join("test_output","sample_12.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1")
        t.add_signal("time", vtype="float")
        t.add_row(time=0.0)

@raises(TypeError)
def testTypeMismatch1():
    with open(os.path.join("test_output","sample_13.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1")
        t.add_signal("time", vtype=float)
        t.add_row(time=1)

@raises(TypeError)
def testTypeMismatch2():
    with open(os.path.join("test_output","sample_14.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1")
        t.add_signal("time", vtype=float)
        t.add_row(1)

@raises(TypeError)
def testTypeMismatch3():
    with open(os.path.join("test_output","sample_15.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        t = wall.add_table(name="T1")
        t.add_signal("time", vtype=bool)
        t.add_row(1)

@raises(NotFinalized)
def testNotFinalField():
    with open(os.path.join("test_output","sample_16.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=True)
        o = wall.add_object(name="O1");
        o.add_fields(x=12.0)

def testMetadata1():
    with open(os.path.join("test_output","sample_17.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, metadata={"a": "bar"}, verbose=True)
        t = wall.add_table(name="T1", metadata={"b": "foo"});
        t.add_signal("time", metadata={"units": "s"})
        t.add_signal("x")
        t.add_signal("y")
        wall.finalize()
        t.add_row(time=0.0, x=1.0, y=2.0)

    with open(os.path.join("test_output","sample_17.wll"), "rb") as fp:
        wall = WallReader(fp)
        assert_equals(wall.metadata,{"a": "bar"})
        t = wall.read_table("T1")
        assert_equals(t.metadata,{"b": "foo"})
        assert_equals(t.var_metadata["time"],{"units": "s"})

@raises(ValueError)
def testBadTransform1():
    with open(os.path.join("test_output","sample_18.wll"), "wb+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, metadata={"a": "bar"}, verbose=True)
        t = wall.add_table(name="T1", metadata={"b": "foo"});
        t.add_signal("time", metadata={"units": "s"})
        t.add_alias("a", of="time", transform=1.0)
        wall.finalize()
        t.add_row(time=0.0)
