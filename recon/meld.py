from bson import BSON
import bz2

from util import _read, _read_compressed

# This is a unique ID that every meld file starts with so
# it can be identified/verified.
MELD_ID = "recon:meld:v1"

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
    def __init__(self, fp, compression=False, verbose=False):
        self.fp = fp
        self.compression = compression
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
            self.fp.write(MELD_ID)
            self.fp.write(bhead)
            self.start = self.fp.tell()
        else:
            if self.verbose:
                print "Rewriting header"
            save = self.fp.tell()
            self.fp.seek(0)
            self.fp.write(MELD_ID)
            self.fp.write(bhead)
            self.fp.seek(save)

    def _write_object(self, obj):
        base = self.fp.tell()
        bdata = self.bson.encode(obj)
        if self.compression:
            c = bz2.BZ2Compressor()
            a = c.compress(bdata)
            b = c.flush()
            bdata = a+b
        blen = len(bdata)
        if self.verbose:
            print "Binary data: "+str(repr(bdata))
            print "Binary len: "+str(blen)
        self.fp.write(bdata)
        return (base, blen)

    def finalize(self):
        self.header = {"tables": {}, "objects": {}}
        for tname in self.tables:
            table = self.tables[tname]
            index = {}
            for sig in table.signals:
                index[sig] = {"ind": -1, "s": 1.0, "off": 1.0}
                if self.compression:
                    index[sig]["len"] = -1
            for alias in table.aliases:
                index[alias] = {"ind": -1,
                                "s": table.aliases[alias]["scale"],
                                "off": table.aliases[alias]["offset"]}
                if self.compression:
                    index[alias]["len"] = -1
            self.header["tables"][tname] = {"v": table.variables, "indices": index}
        for oname in self.objects:
            self.header["objects"][oname] = {"ind": -1}
            if self.compression:
                self.header["objects"][oname]["len"] = -1

        self.header["comp"] = self.compression

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
        self.variables = []
        self.signals = signals
        self.aliases = {}
        self.closed = False
        for s in signals:
            self.variables.append(s)
    def add_alias(self, alias, of, scale=1.0, offset=0.0):
        if alias in self.signals:
            raise NameError("Table already contains a signal named "+alias)
        if alias in self.aliases:
            raise NameError("Table already contains an alias named "+alias)
        if not of in self.signals:
            raise NameError("Alias "+alias+" refers to non-existant signal "+of)
        self.variables.append(alias)
        self.aliases[alias] = {"of": of, "scale": scale, "offset": offset};
    def write(self, sig, data):
        if not self.writer.defined:
            raise MeldNotFinalized("Meld must be finalized before writing data")
        if self.closed:
            raise FinalizedTable("Table "+self.name+" is already closed for writing")
        if not sig in self.signals:
            raise NameError("Cannot write unknown signal "+sig+" to table")

        # TODO: Make sure this is a list
        # TODO: Make sure it is the correct size (matches any previous)
        # TODO: Perform type checks

        (base, blen) = self.writer._write_object({"d": data})
        self.writer._signal_header(self.name, sig)["ind"] = base
        if self.writer.compression:
            self.writer._signal_header(self.name, sig)["len"] = blen
        for alias in self.aliases:
            if self.aliases[alias]["of"]==sig:
                self.writer._signal_header(self.name, alias)["ind"] = base
                if self.writer.compression:
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

        (base, blen) = self.writer._write_object(kwargs)
        self.writer._object_header(self.name)["ind"] = base
        if self.writer.compression:
            self.writer._object_header(self.name)["len"] = base

        # Rewrite header with updated location information
        self.writer._write_header()
        self.writer.cur = None
    def close(self):
        self.closed = True

class MeldReader(object):
    def __init__(self, fp, verbose=False):
        self.fp = fp
        self.verbose = verbose
        file_id = self.fp.read(len(MELD_ID))
        if file_id != MELD_ID:
            raise IOError("File is not a Meld file")
        self.header = _read(self.fp, self.verbose)
        self.compression = self.header["comp"]
        if self.verbose:
            print "Compression: "+str(self.compression)
        if self.verbose:
            print "Header = "+str(self.header)

    def tables(self):
        return self.header["tables"].keys()

    def objects(self):
        return self.header["objects"].keys()

    def read_table(self, table):
        if not table in self.tables():
            raise NameError("No table named "+table+" found");
        return MeldTableReader(self, table)

    def read_object(self, objname):
        if not objname in self.objects():
            raise NameError("No object named "+table+" found");
        ind = self.header["objects"][objname]["ind"]
        self.fp.seek(ind)
        if self.compression:
            blen = self.header["objects"][objname]["len"]
            return _read_compressed(self.fp, self.verbose, blen)
        else:
            return _read(self.fp, self.verbose)

class MeldTableReader(object):
    def __init__(self, reader, table):
        self.reader = reader
        self.table = table
        if not self.table in self.reader.header["tables"]:
            raise NameError("Cannot find table "+self.table)
        self.indices = self.reader.header["tables"][table]["indices"]
        self.signames = self.reader.header["tables"][table]["v"]
        
    def signals(self):
        return self.signames

    def data(self, signal):
        if not signal in self.indices:
            NameError("No signal named "+str(signal)+" found in table "+str(self.table))
        ind = self.indices[signal]["ind"]
        self.reader.fp.seek(ind)
        if self.reader.compression:
            blen = self.indices[signal]["len"]
            return _read_compressed(self.reader.fp, self.reader.verbose, blen)["d"]
        else:
            return _read(self.reader.fp, self.reader.verbose)["d"]
