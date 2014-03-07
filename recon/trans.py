from recon.wall import WallReader, WallWriter
from recon.meld import MeldReader, MeldWriter

def wall2meld(wfp, mfp):
    """
    This function reads a wall file in and converts it to a
    meld file.
    """
    wall = WallReader(wfp)
    meld = MeldWriter(mfp, metadata=wall.metadata)

    objects = {}
    tables = {}

    # TODO: Add option to do overthing in memory? (to avoid multiple reads)
    #       Simple implementation...copy wfp into a StringIO buffer and use that.

    # Step 1: Create definitions for meld
    #   Start with objects...
    for objname in wall.objects():
        obj = wall.read_object(objname)
        objects[objname] = meld.add_object(objname, metadata=obj.metadata)

    #   ...then do tables
    for tabname in wall.tables():
        table = wall.read_table(tabname)
        tables[tabname] = meld.add_table(name=tabname,
                                         metadata=table.metadata)
        mtable = tables[tabname]
        for signal in table.signals():
            mtable.add_signal(signal, metadata=table.var_metadata.get(signal, None))

        for alias in table.aliases():
            mtable.add_alias(alias, 
                             of=table.alias_of(alias),
                             transform=table.alias_transform_string(alias),
                             metadata=table.var_metadata.get(alias, None))
    
    # Now that all definitions are made, we can finalize the meld
    meld.finalize()

    # Step 2: Write actual data in meld
    #   Again, objects first...
    for objname in wall.objects():
        obj = wall.read_object(objname)
        objects[objname].write(**(obj.data))

    #   ...then tables
    for tabname in wall.tables():
        wtable = wall.read_table(tabname)
        mtable = tables[tabname]
        for signal in wtable.signals():
            mtable.write(signal, wtable.data(signal))

    meld.close()

def dsres2meld(df, mfp, verbose=False, compression=True, single=True):
    """
    This function reads in a file in 'dsres' format and then writes it
    back out in meld format.  Note there is a dependency in this code
    on numpy and dymat.
    """
    import numpy
    from DyMat import DyMatFile

    # Read dsres file
    mf = DyMatFile(df)
    # Open a meld file to write to
    meld = MeldWriter(mfp, compression=compression, single=single)

    # Initialize a couple of internal data structures
    tables = {}
    signal_map = {}
    alias_map = {}

    # This is the key to use for "description" fields
    DESC = "desc"

    # We loop over the blocks in the dsres file and each block
    # will end up being a table.
    for block in mf.blocks():
        # Extract the abscissa data for this block
        if verbose:
            print "Block: "+str(block)
        (abscissa, aname, adesc) = mf.abscissa(block)

        # Determine all columns in the dsres file and associate
        # the signals and aliases with these columns
        columns = {}
        if verbose:
            print "Abscissa: "+str(aname)+" '"+adesc+"'"

        signals = []
        aliases = []

        # Loop over all variables in this block and either make them
        # signals or aliases (if some other variable has already
        # claimed that column)
        for name in mf.names(block):
            col = mf._vars[name][2]
            if col in columns:
                if verbose:
                    print "  Alias "+name+" (of: "+columns[col]+")"
                aliases.append((name, mf._vars[name][0],
                                columns[col], mf._vars[name][3]))
            else:
                if verbose:
                    print "  Signal "+name+" ("+str(col)+")"
                columns[col] = name
                signals.append(name)

        if verbose:
            print "Number of columns: "+str(len(columns.keys()))
            print "Number of signals: "+str(len(signals))
            print "Number of aliases: "+str(len(aliases))

        signal_map[block] = signals
        alias_map[block] = aliases

        # Generate table for this block (and store it away for later)
        tables[block] = meld.add_table("T"+str(block))

        # Add abscissa
        tables[block].add_signal(aname, metadata={DESC: adesc})

        for signal in signals:
            tables[block].add_signal(signal, metadata={DESC: mf.description(signal)})

        # Add aliases (and their metadata)
        for alias in aliases:
            transform = None
            if alias[3]<0.0:
                tables[block].add_alias(alias=alias[0], of=alias[2],
                                        transform="aff(-1,0)",
                                        metadata={DESC:mf.description(alias[0])})
            else:
                tables[block].add_alias(alias=alias[0], of=alias[2],
                                        metadata={DESC:mf.description(alias[0])})

    # Finalize structure
    meld.finalize()

    # Now loop again, this time with the intention to write data
    for block in mf.blocks():
        # Write abscissa for this block
        (abscissa, aname, adesc) = mf.abscissa(block)
        abscissa = list(abscissa.astype(numpy.float))
        tables[block].write(aname, list(abscissa))

        signals = signal_map[block]
        print "Block: "+str(block)
        print "  Writing signals: "+str(signals)

        # Then write signals (no need to write aliases)
        for signal in signals:
            vec = list(mf.data(signal).astype(numpy.float))
            tables[block].write(signal, vec)

    # Close the MeldWriter
    meld.close()
