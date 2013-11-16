from bson import BSON

class MeldNotFinalized(Exception):
    """
    Thrown when data is written to a meld that hasn't been finalized.
    """
    pass
    
class FinalizedMeld(Exception):
    """
    Thrown when an attempt is made to change structural definitions
    of a meld file after it has been finalized."
    """
    pass

class FinalizedTable(Exception):
    """
    Thrown when an attempt is made to change structural definitions
    of a meld file after it has been finalized."
    """
    pass

class FinalizedObject(Exception):
    """
    Thrown when an attempt is made to change structural definitions
    of a meld file after it has been finalized."
    """
    pass

class MissingData(Exception):
    """
    Thrown when a Meld file is closed and table or object data is missing
    """
    pass

class MeldWriter(object):
    def __init__(self, fp, verbose=False):
        self.fp = fp
        self.verbose = verbose
        self.tables = {}
        self.objects = {}
        self.cur = None # Current object being written
        self.bson = BSON()

        # Everything after here is set when finalized
        self.defined = False
        self.header = None
        self.start = None

    def _check_names(self, name):
        """
        This checks any new name introduced (for either a table or an object)
        to make sure it is unique across the wall.
        """
        if name in self.tables:
            raise NameError("Wall already contains a table named "+name)
        if name in self.objects:
            raise NameError("Wall already contains an object named "+name)

    def add_table(self, name, signals):
        if self.defined:
            raise FinalizedMeld()
        self._check_names(name)
        table = MeldTableWriter(self, name, signals)
        self.tables[name] = table
        self.cur = table
        return table

    def add_object(self, name):
        if self.defined:
            raise FinalizedMeld()
        self._check_names(name)
        obj = MeldObjectWriter(self, name)
        self.objects[name] = obj
        self.cur = obj
        return obj

    def _signal_header(self, table, signal):
        t = self.header["tables"][table]["indices"]
        if not signal in t:
            raise KeyError("Signal "+signal+" not present in "+str(t.keys()))
        s = t[signal]
        return s

    def _object_header(self, objname):
        return self.header["objects"][objname]

    def _write_header(self):
        # Binary encoding of header
        if self.verbose:
            print "Header = "+str(self.header)
        bhead = self.bson.encode(self.header)
        if self.verbose:
            print "len(bhead) = "+str(len(bhead))

        if self.start==None:
            if self.verbose:
                print "Writing header for the first time"
            self.fp.write(bhead)
            self.start = self.fp.tell()
        else:
            if self.verbose:
                print "Rewriting header"
            save = self.fp.tell()
            self.fp.seek(0)
            self.fp.write(bhead)
            self.fp.seek(save)

    def finalize(self):
        self.header = {"tables": {}, "objects": {}}
        for tname in self.tables:
            table = self.tables[tname]
            index = {}
            for sig in table.signals:
                index[sig] = {"ind": -1, "len": -1, "s": 1.0, "off": 1.0}
            for alias in table.aliases:
                index[alias] = {"ind": -1, "len": -1,
                                "s": table.aliases[alias]["scale"],
                                "off": table.aliases[alias]["offset"]}
            self.header["tables"][tname] = {"indices": index}
        for oname in self.objects:
            self.header["objects"][oname] = {"ind": -1, "len": -1}

        self._write_header()
        self.defined = True

    def close(self):
        if not self.defined:
            self.finalize()
        missing = []
        for table in self.tables:
            for signal in self.tables[table].signals:
                if self._signal_header(table, signal)["ind"]==-1:
                    missing.append(signal)
        for obj in self.objects:
            if self._object_header(obj)["ind"]==-1:
                missing.append(name)
        if len(missing)>0:
            raise MissingData("Data not written for: "+str(missing))


class MeldTableWriter(object):
    def __init__(self, writer, name, signals):
        self.writer = writer
        self.name = name
        self.signals = signals
        self.aliases = {}
        self.closed = False
    def add_alias(self, alias, of, scale=1.0, offset=0.0):
        if alias in self.signals:
            raise NameError("Table already contains a signal named "+alias)
        if alias in self.aliases:
            raise NameError("Table already contains an alias named "+alias)
        if not of in self.signals:
            raise NameError("Alias "+alias+" refers to non-existant signal "+of)
        self.aliases[alias] = {"of": of, "scale": scale, "offset": offset};
    def write(self, sig, data):
        if not self.writer.defined:
            raise MeldNotFinalized("Meld must be finalized before writing data")
        if self.closed:
            raise FinalizedTable("Table "+self.name+" is already closed for writing")
        if not sig in self.signals:
            raise NameError("Cannot write unknown signal "+sig+" to table")
        base = self.writer.fp.tell()
        # TODO: Make sure this is a list
        # TODO: Make sure it is the correct size (matches any previous)
        # TODO: Perform type checks
        bdata = self.writer.bson.encode({"x": data})
        # We can only encode documents with this library, so now we need to strip
        # out the data
        bdata = bdata[(4+3):-1] # This is just the array
        blen = len(bdata)
        if self.writer.verbose:
            print "Data: "+str(data)
            print "Binary Data: "+str(repr(bdata))
            print "Length: "+str(blen)
        self.writer.fp.write(bdata)
        self.writer._signal_header(self.name, sig)["ind"] = base
        self.writer._signal_header(self.name, sig)["len"] = blen
        for alias in self.aliases:
            if self.aliases[alias]["of"]==sig:
                self.writer._signal_header(self.name, alias)["ind"] = base
                self.writer._signal_header(self.name, alias)["len"] = blen
                
        # Rewrite header with updated location information
        self.writer._write_header()
        self.writer.cur = None
    def close(self):
        missing = []
        self.closed = True

class MeldObjectWriter(object):
    def __init__(self, writer, name):
        self.writer = writer
        self.name = name
        self.closed = False
    def write(self, **kwargs):
        if not self.writer.defined:
            raise MeldNotFinalized("Meld must be finalized before writing data")
        if self.closed:
            raise FinalizedObject("Object "+name+" is already closed for writing")
        # TODO: Perform type checks
        base = self.writer.fp.tell()
        bdata = self.writer.bson.encode(kwargs)
        blen = len(bdata)
        if self.writer.verbose:
            print "Binary Data: "+str(repr(bdata))
            print "Length: "+str(blen)
        self.writer.fp.write(bdata)
        self.writer._object_header(self.name)["ind"] = base
        self.writer._object_header(self.name)["len"] = blen

        # Rewrite header with updated location information
        self.writer._write_header()
        self.writer.cur = None
    def close(self):
        self.closed = True

class MeldReader(object):
    def __init__(self, fp, verbose=False):
        pass
