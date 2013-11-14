from bson import BSON

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
            save = fp.tell()
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
        pass

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
        if self.closed:
            raise FinalizedTable("Table "+self.name+" is already closed for writing")
        pass
    def close(self):
        self.closed = True
        pass

class MeldObjectWriter(object):
    def __init__(self, writer, name):
        self.writer = writer
        self.name = name
        self.closed = False
    def write(self, **kwargs):
        if self.closed:
            raise FinalizedTable("Object "+name+" is already closed for writing")
        pass
    def close(self):
        self.closed = True

class MeldReader(object):
    def __init__(self, fp, verbose=False):
        pass
