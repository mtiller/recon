
def write_file():
    from nido import WallWriter
    with open("sample.nco", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp)

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
        t.add_row(time=2.0, x=-1.0, y=3.0)
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

def read_file():
    from nido import WallReader
    with open("sample.nco") as fp:
        wall = WallReader(fp)
        print "Objects:"
        for objname in wall.objects():
            obj = wall.read_object(objname)
            print "  "+objname+" = "+str(obj)
        print "Tables:"
        for tabname in wall.tables():
            table = wall.read_table(tabname)
            print "  "+tabname
            for signal in table:
                print "    "+signal+" = "+table[signal]

write_file()
