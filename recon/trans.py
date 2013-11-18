from recon.wall import WallReader, WallWriter
from recon.meld import MeldReader, MeldWriter

def wall2meld(wfp, mfp):
    """
    This function reads a wall file in and converts it to a
    meld file.
    """
    wall = WallReader(wfp)
    meld = MeldWriter(mfp)

    meld.metadata.update(wall.metadata)

    objects = {}
    tables = {}

    # TODO: Add option to do overthing in memory? (to avoid multiple reads)
    #       Simple implementation...copy wfp into a StringIO buffer and use that.

    # Step 1: Create definitions for meld
    #   Start with objects...
    for objname in wall.objects():
        objects[objname] = meld.add_object(objname)

    #   ...then do tables
    for tabname in wall.tables():
        table = wall.read_table(tabname)
        tables[tabname] = meld.add_table(name=tabname, signals=table.signals());
        mtable = tables[tabname]
        mtable.metadata.update(table.metadata)
        for k in table.var_metadata:
            mtable.set_var_metadata(k, **table.var_metadata[k])
        for alias in table.aliases():
            mtable.add_alias(alias, 
                             of=table.alias_of(alias),
                             scale=table.alias_scale(alias),
                             offset=table.alias_offset(alias))
    
    # Now that all definitions are made, we can finalize the meld
    meld.finalize()

    # Step 2: Write actual data in meld
    #   Again, objects first...
    for objname in wall.objects():
        obj = wall.read_object(objname)
        objects[objname].write(**obj)

    #   ...then tables
    for tabname in wall.tables():
        wtable = wall.read_table(tabname)
        mtable = tables[tabname]
        for signal in wtable.signals():
            mtable.write(signal, wtable.data(signal));

def dsres2meld(df, mfp, verbose=False, compression=True, single=True):
    """
    This function reads in a file in 'dsres' format and then write is
    back out in meld format.  Note there is a dependency in this code
    on numpy and dymat.
    """
    import numpy
    from dymat import DyMatFile

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
        tables[block] = meld.add_table("T"+str(block), signals=[aname]+signals)
        tables[block].set_var_metadata(aname, **{DESC:adesc})

        # Add metadata for signals
        for name in signals:
            tables[block].set_var_metadata(name,
                                           **{DESC:mf.description(name)})

        # ...and then add aliases (and their metadata)
        for alias in aliases:
            tables[block].add_alias(alias=alias[0], of=alias[2],
                                    scale=alias[3], offset=0.0)
            tables[block].set_var_metadata(alias[0],
                                           **{DESC:mf.description(alias[0])})

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
