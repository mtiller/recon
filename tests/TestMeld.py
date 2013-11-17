def write_meld(compression=False, verbose=False,n=0,name="sample"):
    from recon.meld import MeldWriter

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

def testUncompressedValidMeld():
    write_meld(verbose=False,compression=False,n=100)
    read_meld(verbose=False)

def testCompressedValidMeld():
    write_meld(verbose=False,compression=True,name="sample_comp",n=100)
    read_meld(verbose=False)
