import sys

from serial import BSONSerializer, MsgPackSerializer

from util import write_len, read_len

#DEFSER = BSONSerializer
DEFSER = MsgPackSerializer

# This is a unique ID that every meld file starts with so
# it can be identified/verified.
MELD_ID = "recon:meld:v1"

# Meld
TABLES = "tables"
OBJECTS = "objects"
COMP = "comp"

# Tables
INDICES = "indices"
VARIABLES = "v"
METADATA = "metadata"
VMETADATA = "var_metadata"

# Signal
INDEX = "ind"
LENGTH = "len"

# Alias
OF = "of"
SCALE = "scale"
OFFSET = "offset"

# Columne
DATA = "d"

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

class WriteAfterClose(Exception):
    """
    Thrown when there is an attempt to write to a closed meld
    """
    pass

class MeldWriter(object):
    def __init__(self, fp, compression=False, verbose=False):
        self.fp = fp
        self.verbose = verbose
        self.compression = compression
        #self.compression = False
        self.tables = {}
        self.objects = {}
        self.metadata = {}
        self.cur = None # Current object being written
        #self.ser = BSONSerializer(compress=self.compression)
        self.ser = DEFSER(compress=self.compression)

        # Everything after here is set when finalized
        self.defined = False
        self.header = None
        self.start = None
        self.headlen = None
        self.closed = False

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
        t = self.header[TABLES][table][INDICES]
        s = t[signal]
        return s

    def _object_header(self, objname):
        return self.header[OBJECTS][objname]

    def _write_header(self):
        # Binary encoding of header
        if self.verbose:
            print "Header = "+str(self.header)

        # Header can never be compressed because it needs to
        # remain the same size on each write
        bhead = self.ser.encode(self.header, uncomp=True)
        blen = len(bhead)
        if self.verbose:
            print "len(bhead) = "+str(blen)

        if self.start==None:
            if self.verbose:
                print "Writing header for the first time"
            self.fp.write(MELD_ID)
            write_len(self.fp, blen)
            print "BLEN -> "+str(blen)
            self.fp.write(bhead)
            self.start = self.fp.tell()
            self.headlen = blen
        else:
            if self.verbose:
                print "Rewriting header"
            if blen!=self.headlen:
                raise IOError("Header length changed on rewrite")
            save = self.fp.tell()
            self.fp.seek(0)
            self.fp.write(MELD_ID)
            write_len(self.fp, blen)
            self.fp.write(bhead)
            self.fp.seek(save)

    def _write_object(self, obj):
        base = self.fp.tell()
        print "Writing @ = "+str(base)
        bdata = self.ser.encode(obj)
        blen = len(bdata)
        if self.verbose:
            print "Binary data: "+str(repr(bdata))
            print "Binary len: "+str(blen)
        self.fp.write(bdata)
        return (base, blen)

    def finalize(self):
        self.header = {TABLES: {}, OBJECTS: {},
                       METADATA: self.metadata}
        for tname in self.tables:
            table = self.tables[tname]
            index = {}
            for sig in table.signals:
                index[sig] = {INDEX: -1L, LENGTH: -1L, SCALE: 1.0, OFFSET: 0.0}
            for alias in table.aliases:
                index[alias] = {INDEX: -1L,
                                LENGTH: -1L,
                                SCALE: table.aliases[alias][SCALE],
                                OFFSET: table.aliases[alias][OFFSET]}
            self.header[TABLES][tname] = {VARIABLES: table.variables, INDICES: index,
                                          METADATA: table.metadata,
                                          VMETADATA: table._vmd}
        for oname in self.objects:
            self.header[OBJECTS][oname] = {INDEX: -1L, LENGTH: -1L}

        self.header[COMP] = self.compression

        self._write_header()
        self.defined = True

    def close(self):
        if not self.defined:
            self.finalize()
        missing = []
        for table in self.tables:
            for signal in self.tables[table].signals:
                if self._signal_header(table, signal)[INDEX]==-1:
                    missing.append(signal)
        for obj in self.objects:
            if self._object_header(obj)[INDEX]==-1:
                missing.append(name)
        if len(missing)>0:
            raise MissingData("Data not written for: "+str(missing))
        self.closed = True

class MeldTableWriter(object):
    def __init__(self, writer, name, signals):
        self.writer = writer
        self.name = name
        self.variables = []
        self.signals = signals
        self.aliases = {}
        self.metadata = {}
        self._vmd = {}
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
        self.aliases[alias] = {OF: of, SCALE: scale, OFFSET: offset};

    def set_var_metadata(self, name, **kwargs):
        if not name in self.signals and not name in self.aliases:
            raise NameError("No such signal: "+name);
        if not name in self._vmd:
            self._vmd[name] = {}

        self._vmd[name].update(kwargs)

    def write(self, sig, data):
        if not self.writer.defined:
            raise MeldNotFinalized("Meld must be finalized before writing data")
        if not sig in self.signals:
            raise NameError("Cannot write unknown signal "+sig+" to table")
        if self.writer._signal_header(self.name, sig)[INDEX]!=-1:
            raise WriteAfterClose("Signal "+sig+" has already been written")
        if not type(data)==list:
            raise ValueError("Data for signal "+sig+" must be a list")

        # TODO: Make sure it is the correct size (matches any previous)
        # TODO: Perform type checks

        (base, blen) = self.writer._write_object({DATA: data})
        self.writer._signal_header(self.name, sig)[INDEX] = long(base)
        self.writer._signal_header(self.name, sig)[LENGTH] = long(blen)
        for alias in self.aliases:
            if self.aliases[alias][OF]==sig:
                self.writer._signal_header(self.name, alias)[INDEX] = long(base)
                self.writer._signal_header(self.name, alias)[LENGTH] = long(blen)
                
        # Rewrite header with updated location information
        self.writer._write_header()
        self.writer.cur = None

class MeldObjectWriter(object):
    def __init__(self, writer, name):
        self.writer = writer
        self.name = name
    def write(self, **kwargs):
        if not self.writer.defined:
            raise MeldNotFinalized("Meld must be finalized before writing data")
        if self.writer._object_header(self.name)[INDEX] != -1:
            raise WriteAfterClose("Object "+self.name+" is closed for writing")

        (base, blen) = self.writer._write_object(kwargs)
        self.writer._object_header(self.name)[INDEX] = long(base)
        self.writer._object_header(self.name)[LENGTH] = long(blen)

        # Rewrite header with updated location information
        self.writer._write_header()
        self.writer.cur = None

class MeldReader(object):
    def __init__(self, fp, verbose=False):
        self.fp = fp
        self.verbose = verbose

        self.ser = DEFSER(compress=False)

        file_id = self.fp.read(len(MELD_ID))
        if file_id != MELD_ID:
            raise IOError("File is not a Meld file")

        blen = read_len(self.fp)
        self.headlen = blen
        print "BLEN <- "+str(blen)
        self.header = self.ser.decode(self.fp, length=blen)
        self.metadata = self.header[METADATA]
        self.compression = self.header[COMP]
        #self.ser = BSONSerializer(compress=self.compression)
        self.ser = DEFSER(compress=self.compression)
        if self.verbose:
            print "Compression: "+str(self.compression)
        if self.verbose:
            print "Header = "+str(self.header)

    def report(self):
        ret = {}
        ret["header"] = self.headlen
        
        for table in self.tables():
            signals = self.header[TABLES][table][INDICES].keys()
            signal_map = {}
            tl = 0
            for signal in signals:
                ind = self.header[TABLES][table][INDICES][signal][INDEX]
                blen = self.header[TABLES][table][INDICES][signal][LENGTH]
                if not ind in signal_map:
                    signal_map[ind] = [blen]
                    tl += blen
                signal_map[ind].append(signal)
            ret[table] = signal_map
        return ret

    def tables(self):
        return self.header[TABLES].keys()

    def objects(self):
        return self.header[OBJECTS].keys()

    def read_table(self, table):
        if not table in self.tables():
            raise NameError("No table named "+table+" found");
        return MeldTableReader(self, table)

    def read_object(self, objname):
        if not objname in self.objects():
            raise NameError("No object named "+table+" found");
        ind = self.header[OBJECTS][objname][INDEX]
        blen = self.header[OBJECTS][objname][LENGTH]
        self.fp.seek(ind)
        print "Reading object: "+objname
        print "  Index: "+str(ind)
        print "  Length: "+str(blen)
        return self.ser.decode(self.fp, blen)

class MeldTableReader(object):
    def __init__(self, reader, table):
        self.reader = reader
        self.table = table
        if not self.table in self.reader.header[TABLES]:
            raise NameError("Cannot find table "+self.table)
        self.indices = self.reader.header[TABLES][table][INDICES]
        self.signames = self.reader.header[TABLES][table][VARIABLES]
        self.metadata = self.reader.header[TABLES][table][METADATA]
        self.var_metadata = self.reader.header[TABLES][table][VMETADATA]
    def signals(self):
        return self.signames

    def data(self, signal):
        if not signal in self.indices:
            raise NameError("No signal named "+str(signal)+" found in table "+str(self.table))
        ind = self.indices[signal][INDEX]
        blen = self.indices[signal][LENGTH]
        scale = self.indices[signal][SCALE]
        offset = self.indices[signal][OFFSET]
        self.reader.fp.seek(ind)
        data = self.reader.ser.decode(self.reader.fp, blen)[DATA]
        return map(lambda x: x*scale+offset, data)
