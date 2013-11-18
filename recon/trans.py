from recon.wall import WallReader, WallWriter
from recon.meld import MeldReader, MeldWriter

def wall2meld(wfp, mfp):
    wall = WallReader(wfp)
    meld = MeldWriter(mfp)

    meld.metadata.update(wall.metadata)

    objects = {}
    tables = {}

    # Create definitions for meld

    # TODO: Process metadata
    # TODO: Add option to do overthing in memory? (to avoid multiple reads)
    #       Simple implementation...copy wfp into a StringIO buffer and use that.

    # Start with objects
    for objname in wall.objects():
        objects[objname] = meld.add_object(objname)

    # Then do tables
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

    # Write actual data in meld
    for objname in wall.objects():
        obj = wall.read_object(objname)
        objects[objname].write(**obj)

    for tabname in wall.tables():
        wtable = wall.read_table(tabname)
        mtable = tables[tabname]
        for signal in wtable.signals():
            mtable.write(signal, wtable.data(signal));

def dsres2meld(df, mfp, verbose=False, compression=True, single=True):
    from dymat import DyMatFile
    import numpy

    mf = DyMatFile(df)
    meld = MeldWriter(mfp, compression=compression, single=single)

    tables = {}
    signal_map = {}
    alias_map = {}

    DESC = "desc"

    # Definitions
    for block in mf.blocks():
        columns = {}
        if verbose:
            print "Block: "+str(block)
        (abscissa, aname, adesc) = mf.abscissa(block)
        if verbose:
            print "Abscissa: "+str(aname)+" '"+adesc+"'"

        signals = []
        aliases = []

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

        print "Number of columns: "+str(len(columns.keys()))
        print "Number of signals: "+str(len(signals))
        print "Number of aliases: "+str(len(aliases))

        signal_map[block] = signals
        alias_map[block] = aliases

        tables[block] = meld.add_table("T"+str(block), signals=[aname]+signals)
        tables[block].set_var_metadata(aname, **{DESC:adesc})

        for name in signals:
            tables[block].set_var_metadata(name,
                                           **{DESC:mf.description(name)})
        for alias in aliases:
            tables[block].add_alias(alias=alias[0], of=alias[2],
                                    scale=alias[3], offset=0.0)
            tables[block].set_var_metadata(alias[0],
                                           **{DESC:mf.description(alias[0])})

    meld.finalize()

    for block in mf.blocks():
        (abscissa, aname, adesc) = mf.abscissa(block)
        abscissa = list(abscissa.astype(numpy.float))
        tables[block].write(aname, list(abscissa))

        signals = signal_map[block]
        print "Block: "+str(block)
        print "  Writing signals: "+str(signals)

        for signal in signals:
            vec = list(mf.data(signal).astype(numpy.float))
            tables[block].write(signal, vec)

    meld.close()
