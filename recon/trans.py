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

def dsres2meld(df, mfp, verbose=False, compression=True):
    from dymat import DyMatFile

    mf = DyMatFile(df)
    meld = MeldWriter(mfp, compression=compression)

    tables = {}
    signal_map = {}
    alias_map = {}

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
                aliases.append((name, columns[col], mf._vars[name][3]))
            else:
                if verbose:
                    print "  Signal "+name+" ("+str(col)+")"
                columns[col] = name
                signals.append(name)

        signal_map[block] = signals
        alias_map[block] = aliases

        tables[block] = meld.add_table("T"+str(block), signals=[aname]+signals)
        print "  Signals: "+str(tables[block].signals)
        tables[block].set_var_metadata(aname, description=adesc)

        for name in signals:
            tables[block].set_var_metadata(name, description=mf.description(name))
        for name in aliases:
            tables[block].add_alias(alias=name[0], of=name[1], scale=name[2], offset=0.0)
            tables[block].set_var_metadata(name, description=mf.description(name[0]))

    meld.finalize()

    for block in mf.blocks():
        (abscissa, aname, adesc) = mf.abscissa(block)
        tables[block].write(aname, list(abscissa))

        signals = signal_map[block]
        print "Block: "+str(block)
        print "  Writing signals: "+str(signals)

        for signal in signals:
            tables[block].write(signal, list(mf.data(signal)))

    meld.close()
