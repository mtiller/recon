def write_wall():
    from recon.wall import WallWriter

    with open("sample.wll", "w+") as fp:
        # Create the wall object with a file-like object to write to
        wall = WallWriter(fp, verbose=False)

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

def read_wall():
    from recon.wall import WallReader

    with open("sample.wll", "rb") as fp:
        wall = WallReader(fp, verbose=False)

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

def write_meld():
    from recon.meld import MeldWriter

    with open("sample.mld", "w+") as fp:
        meld = MeldWriter(fp, verbose=False)
        
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
        t.write("time", [0.0, 1.0, 2.0]);
        t.write("x", [1.0, 0.0, 1.0]);
        t.write("y", [2.0, 3.0, 3.0]);

        # Once we switch to writing another entity, the previous entity is
        # implicitly finalized
        obj1.write(nationality="American", name="Mike");

        obj2.write(nationality="GreatBritisher", name="Pete");

        # When the meld is closed (and it must be closed!), the entity that was
        # currently being written is finalized
        meld.close()

def read_meld(verbose=True):
    from recon.meld import MeldReader

    with open("sample.mld", "rb") as fp:
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

write_wall()
read_wall()

write_meld()
read_meld(False)
