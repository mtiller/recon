import sys

from serial import BSONSerializer, MsgPackSerializer

from util import write_len, read_len, conv_len

#DEFSER = BSONSerializer
DEFSER = MsgPackSerializer

# This is a unique ID that every meld file starts with so
# it can be identified/verified.
MELD_ID = "recon:meld:v1"

# Meld
H_TABLES = "t"
H_OBJECTS = "o"
H_COMP = "c"
H_METADATA = "m"

# Tables
T_INDICES = "i"
T_VARIABLES = "V"
T_METADATA = "m"
T_VMETADATA = "v"

# Variables
V_INDEX = "i"
V_INDHOLD = b'\x00\x00\x00\x00'
V_LENGTH = "l"
V_SCALE = "s"
V_OFFSET = "o"

# Alias
A_OF = "v"

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
    def __init__(self, fp, compression=False, verbose=False, single=False):
        self.fp = fp
        self.verbose = verbose
        self.compression = compression
        self.tables = {}
        self.objects = {}
        self.metadata = {}
        self.cur = None # Current object being written
        self.ser = DEFSER(compress=self.compression, single=True)

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
        t = self.header[H_TABLES][table][T_INDICES]
        s = t[signal]
        return s

    def _object_header(self, objname):
        return self.header[H_OBJECTS][objname]

    def _write_header(self):
        # Binary encoding of header
        if self.verbose:
            print "Header = "+str(self.header)

        # Header can never be compressed because it needs to
        # remain the same size on each write
        bhead = self.ser.encode_obj(self.header, uncomp=True)
        blen = len(bhead)
        if self.verbose:
            print "len(bhead) = "+str(blen)

        if self.start==None:
            if self.verbose:
                print "Writing header for the first time"
            self.fp.write(MELD_ID)
            write_len(self.fp, blen)
            self.fp.write(bhead)
            self.start = self.fp.tell()
            self.headlen = blen
        else:
            if self.verbose:
                print "Rewriting header"
            if blen>self.headlen:
                raise IOError("Header length increased on rewrite")
            save = self.fp.tell()
            self.fp.seek(0)
            self.fp.write(MELD_ID)
            write_len(self.fp, blen)
            self.fp.write(bhead)
            self.fp.seek(save)

    def _write_object(self, obj):
        base = self.fp.tell()
        bdata = self.ser.encode_obj(obj)
        blen = len(bdata)
        if self.verbose:
            print "Binary data: "+str(repr(bdata))
            print "Binary len: "+str(blen)
        self.fp.write(bdata)
        return (base, blen)

    def _write_vector(self, vec):
        base = self.fp.tell()
        bdata = self.ser.encode_vec(vec)
        blen = len(bdata)
        if self.verbose:
            print "Binary data: "+str(repr(bdata))
            print "Binary len: "+str(blen)
        self.fp.write(bdata)
        return (base, blen)

    def finalize(self):
        self.header = {H_TABLES: {},
                       H_OBJECTS: {},
                       H_METADATA: self.metadata}
        for tname in self.tables:
            table = self.tables[tname]
            index = {}
            for sig in table.signals:
                index[sig] = {V_INDEX: V_INDHOLD,
                              V_LENGTH: V_INDHOLD,
                              V_SCALE: 1.0, V_OFFSET: 0.0}
            for alias in table.aliases:
                index[alias] = {V_INDEX: V_INDHOLD,
                                V_LENGTH: V_INDHOLD,
                                V_SCALE: table.aliases[alias][V_SCALE],
                                V_OFFSET: table.aliases[alias][V_OFFSET]}
            self.header[H_TABLES][tname] = {T_VARIABLES: table.variables,
                                            T_INDICES: index,
                                            T_METADATA: table.metadata,
                                            T_VMETADATA: table._vmd}
        for oname in self.objects:
            self.header[H_OBJECTS][oname] = {V_INDEX: V_INDHOLD,
                                             V_LENGTH: V_INDHOLD}

        self.header[H_COMP] = self.compression

        self._write_header()
        self.defined = True

    def close(self):
        if not self.defined:
            self.finalize()
        missing = []
        for table in self.tables:
            for signal in self.tables[table].signals:
                if self._signal_header(table, signal)[V_INDEX]==V_INDHOLD:
                    missing.append(signal)
        for obj in self.objects:
            if self._object_header(obj)[V_INDEX]==V_INDHOLD:
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
        self.aliases[alias] = {A_OF: of, V_SCALE: scale, V_OFFSET: offset};

    def set_var_metadata(self, name, **kwargs):
        if not name in self.signals and not name in self.aliases:
            raise NameError("No such signal: "+str(name));
        if not name in self._vmd:
            self._vmd[name] = {}

        self._vmd[name].update(kwargs)

    def write(self, sig, data):
        if not self.writer.defined:
            raise MeldNotFinalized("Meld must be finalized before writing data")
        if not sig in self.signals:
            raise NameError("Cannot write unknown signal "+sig+" to table")
        if self.writer._signal_header(self.name, sig)[V_INDEX]!=V_INDHOLD:
            raise WriteAfterClose("Signal "+sig+" has already been written")
        if not type(data)==list:
            raise ValueError("Data for signal "+sig+" must be a list")

        # TODO: Make sure it is the correct size (matches any previous)
        # TODO: Perform type checks

        (base, blen) = self.writer._write_vector(data)
        self.writer._signal_header(self.name, sig)[V_INDEX] = long(base)
        self.writer._signal_header(self.name, sig)[V_LENGTH] = long(blen)
        for alias in self.aliases:
            if self.aliases[alias][A_OF]==sig:
                self.writer._signal_header(self.name, alias)[V_INDEX] = long(base)
                self.writer._signal_header(self.name, alias)[V_LENGTH] = long(blen)
                
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
        if self.writer._object_header(self.name)[V_INDEX] != V_INDHOLD:
            raise WriteAfterClose("Object "+self.name+" is closed for writing")

        (base, blen) = self.writer._write_object(kwargs)
        self.writer._object_header(self.name)[V_INDEX] = long(base)
        self.writer._object_header(self.name)[V_LENGTH] = long(blen)

        # Rewrite header with updated location information
        self.writer._write_header()
        self.writer.cur = None

class MeldReader(object):
    def __init__(self, fp, verbose=False):
        self.fp = fp
        self.verbose = verbose

        self.ser = DEFSER(compress=False)

        lead = self.fp.read(len(MELD_ID)+4)
        
        file_id = lead[:-4]
        if file_id != MELD_ID:
            raise IOError("File is not a Meld file")

        blen = conv_len(lead[-4:])
        self.headlen = blen
        self.header = self.ser.decode_obj(self.fp, length=blen)
        self.metadata = self.header[H_METADATA]
        self.compression = self.header[H_COMP]
        self.ser = DEFSER(compress=self.compression)
        if self.verbose:
            print "Compression: "+str(self.compression)
        if self.verbose:
            print "Header = "+str(self.header)

    def report(self):
        ret = {}
        ret["header"] = self.headlen
        
        for table in self.tables():
            signals = self.header[H_TABLES][table][T_INDICES].keys()
            signal_map = {}
            tl = 0
            for signal in signals:
                ind = self.header[H_TABLES][table][T_INDICES][signal][V_INDEX]
                blen = self.header[H_TABLES][table][T_INDICES][signal][V_LENGTH]
                if not ind in signal_map:
                    signal_map[ind] = [blen]
                    tl += blen
                signal_map[ind].append(signal)
            ret[table] = signal_map
        return ret

    def tables(self):
        return self.header[H_TABLES].keys()

    def objects(self):
        return self.header[H_OBJECTS].keys()

    def read_table(self, table):
        if not table in self.tables():
            raise NameError("No table named "+table+" found");
        return MeldTableReader(self, table)

    def read_object(self, objname):
        if not objname in self.objects():
            raise NameError("No object named "+table+" found");
        ind = self.header[H_OBJECTS][objname][V_INDEX]
        blen = self.header[H_OBJECTS][objname][V_LENGTH]
        self.fp.seek(ind)
        return self.ser.decode_obj(self.fp, blen)

class MeldTableReader(object):
    def __init__(self, reader, table):
        self.reader = reader
        self.table = table
        if not self.table in self.reader.header[H_TABLES]:
            raise NameError("Cannot find table "+self.table)
        self.indices = self.reader.header[H_TABLES][table][T_INDICES]
        self.signames = self.reader.header[H_TABLES][table][T_VARIABLES]
        self.metadata = self.reader.header[H_TABLES][table][T_METADATA]
        self.var_metadata = self.reader.header[H_TABLES][table][T_VMETADATA]
    def signals(self):
        return self.signames

    def data(self, signal):
        if not signal in self.indices:
            raise NameError("No signal named "+str(signal)+" found in table "+str(self.table))
        ind = self.indices[signal][V_INDEX]
        blen = self.indices[signal][V_LENGTH]
        scale = self.indices[signal][V_SCALE]
        offset = self.indices[signal][V_OFFSET]
        self.reader.fp.seek(ind)
        data = self.reader.ser.decode_vec(self.reader.fp, blen)
        return map(lambda x: x*scale+offset, data)
