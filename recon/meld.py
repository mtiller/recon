import sys

from serial import BSONSerializer, MsgPackSerializer, UMsgPackSerializer

from util import write_len, read_len, conv_len, parse_transform

#DEFSER = BSONSerializer
DEFSER = MsgPackSerializer
#DEFSER = UMsgPackSerializer

# This is a unique ID that every meld file starts with so
# it can be identified/verified.
MELD_ID = "recon:meld:v01"

# Meld
H_METADATA = "fmeta"
H_TABLES = "tabs"
H_OBJECTS = "objs"
H_COMP = "comp"

# Tables
T_INDICES = "toff"
T_VARIABLES = "vars"
T_METADATA = "tmeta"
T_VMETADATA = "vmeta"

# Variables
V_INDEX = "i"
V_INDHOLD = b'\x00\x00\x00\x00'
V_LENGTH = "l"
V_TRANS = "t"

# Alias
A_OF = "s"

# Object
O_METADATA = "ometa"

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
    def __init__(self, file, metadata={}, compression=False,
                 verbose=False, single=False):
        """
        This is the constructor for the meld writer.

        Note: All metadata must be supplied at the time when the meld
        is created.
        """
        # If a file name is passed, open the file...in *binary* mode
        if type(file)==str:
            fp = open(file, "wb")
        else:
            fp = file # Assume this is a file

        self.fp = fp
        self.verbose = verbose
        self.compression = compression
        self.tables = {} # table name -> MeldTableWriter
        self.objects = {} # object name -> MeldObjectWriter
        self._metadata = metadata
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

    def add_table(self, name, metadata={}):
        """
        Method for adding a new table to the meld.

        Note: All metadata must be supplied at the time the meld is created.
        """
        if self.defined:
            raise FinalizedMeld()
        self._check_names(name)
        table = MeldTableWriter(self, name, metadata)
        self.tables[name] = table
        return table

    def add_object(self, name, metadata={}):
        """
        Method for adding a new object to the meld

        Note: All metadata must be supplied at the time the object
        is created.
        """
        if self.defined:
            raise FinalizedMeld()
        self._check_names(name)
        obj = MeldObjectWriter(self, name, metadata)
        self.objects[name] = metadata
        return obj

    def _signal_header(self, table, signal):
        """
        Extracts header information about a given signal
        in a given table.
        """
        t = self.header[H_TABLES][table][T_INDICES]
        s = t[signal]
        return s

    def _object_header(self, objname):
        """
        Extracts header information about a given object
        """
        return self.header[H_OBJECTS][objname]

    def _write_header(self):
        """
        This method writes the header to the file.  If this is the first
        time that the header is being written then some additional
        state is set (offset of data section, etc).
        """

        # Binary encoding of header
        if self.verbose:
            print "Header = "+str(self.header)

        # Header can never be compressed because it cannot
        # grow in size on subsequent rewrites
        bhead = self.ser.encode_obj(self.header, uncomp=True)
        blen = len(bhead)
        if self.verbose:
            print "len(bhead) = "+str(blen)

        if self.start==None:
            # If we have not written the header previously...
            if self.verbose:
                print "Writing header for the first time"
            self.fp.write(MELD_ID)
            write_len(self.fp, blen)
            self.fp.write(bhead)
            self.start = self.fp.tell()
            self.headlen = blen
        else:
            # If this is a rewrite of the header...
            if self.verbose:
                print "Rewriting header"
            if blen>self.headlen: # pragma: no cover
                raise IOError("Header length increased on rewrite")
            # Save where we are
            save = self.fp.tell()
            # Jump back to the start of the file
            self.fp.seek(0)
            # Rewrite the header
            self.fp.write(MELD_ID)
            write_len(self.fp, blen)
            self.fp.write(bhead)
            # Jump back to where we were when this function was called
            self.fp.seek(save)

    def _write_object(self, obj):
        """
        Code to write an object to the stream
        """
        base = self.fp.tell()
        bdata = self.ser.encode_obj(obj)
        blen = len(bdata)
        if self.verbose:
            print "Binary data: "+str(repr(bdata))
            print "Binary len: "+str(blen)
        self.fp.write(bdata)
        return (base, blen)

    def _write_vector(self, vec):
        """
        Code to write a vector of data to the stream
        """
        base = self.fp.tell()
        bdata = self.ser.encode_vec(vec)
        blen = len(bdata)
        if self.verbose:
            print "Binary data: "+str(repr(bdata))
            print "Binary len: "+str(blen)
        self.fp.write(bdata)
        return (base, blen)

    def finalize(self):
        """
        Finalize the meld (i.e. the header structure).  We may change
        values in the header in the future (in fact, we almost certainly
        will), but the size and structure of the header cannot change.
        """
        self.header = {H_TABLES: {},
                       H_OBJECTS: {},
                       H_METADATA: self._metadata}
        for tname in self.tables:
            table = self.tables[tname]
            index = {} # var name -> header info
            for sig in table.signals:
                index[sig] = {V_INDEX: V_INDHOLD,
                              V_LENGTH: V_INDHOLD}
            for alias in table.aliases:
                index[alias] = {V_INDEX: V_INDHOLD,
                                V_LENGTH: V_INDHOLD}
                if V_TRANS in table.aliases[alias]:
                    index[alias][V_TRANS] = table.aliases[alias][V_TRANS]
            self.header[H_TABLES][tname] = {T_VARIABLES: table.variables,
                                            T_INDICES: index,
                                            T_METADATA: table._metadata,
                                            T_VMETADATA: table._vmd}
        for oname in self.objects:
            self.header[H_OBJECTS][oname] = {V_INDEX: V_INDHOLD,
                                             V_LENGTH: V_INDHOLD,
                                             O_METADATA: self.objects[oname]}

        self.header[H_COMP] = self.compression

        self._write_header()
        self.defined = True

    def close(self):
        """
        Close this meld for any more writing.
        """
        self._write_header()
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
    """
    This class is used to write tables to a meld
    """
    def __init__(self, writer, name, metadata={}):
        """
        Initialized by MeldWriter with information about
        this particular table.
        """
        self.writer = writer
        self.name = name
        self.variables = []
        self.signals = set() # Big performance gain from this
        self.aliases = {} # alias -> alias info
        self.alias_map = {} # signal -> list(aliases)
        self._metadata = metadata
        self._vmd = {} # signal -> metadata
        self._vtypes = {} # signal -> type

    def _check_name(self, name):
        """
        Checks to make sure any new name is unique.
        """
        if name in self.signals:
            raise NameError("Table already contains a signal named "+name)
        if name in self.aliases:
            raise NameError("Table already contains an alias named "+name)

    def add_signal(self, name, metadata=None, vtype=None):
        """
        Used to add a signal to a table.

        Note: All metadata must be supplied at the time the signal
        is added.
        """
        if self.writer.defined:
            raise FinalizedMeld()

        self._check_name(name)
        self.variables.append(name)
        self.signals.add(name)
        if metadata!=None:
            self._vmd[name] = metadata
        if vtype!=None:
            if type(vtype)!=type:
                raise TypeError("Type specifier '"+str(vtype)+"' is not a type")
            self._vtypes[name] = vtype

    def add_alias(self, alias, of, transform=None, metadata=None):
        """
        Code to add an alias to this table.

        Note: All metadata must be supplied at the time the alias
        is added.
        """
        self._check_name(alias)
        if not of in self.signals:
            raise NameError("Alias "+alias+" refers to non-existant signal "+of)
        self.variables.append(alias)

        # Base info data
        self.aliases[alias] = {A_OF: of}

        # Add this alias to the list of aliases associated with the
        # 'of' signal
        if not of in self.alias_map:
            self.alias_map[of] = []
        self.alias_map[of].append(alias)

        # Process any transformation associated with this alias
        if transform!=None:
            if parse_transform(transform)==None:
                raise ValueError("Transform '"+str(transform)+ \
                                     "' could not be parsed")
            self.aliases[alias][V_TRANS] = transform

        # If metadata was supplied, associated it with the alias name
        if metadata!=None:
            self._vmd[alias] = metadata

    def write(self, sig, data):
        """
        Used to write data (i.e. a column) to this table.
        """
        if not self.writer.defined:
            raise MeldNotFinalized("Meld must be finalized before writing data")
        if not sig in self.signals:
            raise NameError("Cannot write unknown signal "+sig+" to table")
        if self.writer._signal_header(self.name, sig)[V_INDEX]!=V_INDHOLD:
            raise WriteAfterClose("Signal "+sig+" has already been written")
        if not type(data)==list:
            raise ValueError("Data for signal "+sig+" must be a list")

        # TODO: Make sure it is the correct size (matches any previous)
        if sig in self._vtypes:
            for val in data:
                if type(val)!=self._vtypes[sig]:
                    raise TypeError("Value in '%s' (%s:%s) doesn't match expected type %s" % \
                                    (sig, str(val), str(type(val)), str(self._vtypes[sig])))

        # Write the data to disk and return the index and length
        (base, blen) = self.writer._write_vector(data)

        # Get the header for this signal and fill in the index and length
        sighead = self.writer._signal_header(self.name, sig)
        sighead[V_INDEX] = long(base)
        sighead[V_LENGTH] = long(blen)

        # Any aliases associated with this signal can now have their
        # index and length filled in as well
        for alias in self.alias_map.get(sig, []):
            ahead = self.writer._signal_header(self.name, alias)
            ahead[V_INDEX] = long(base)
            ahead[V_LENGTH] = long(blen)

class MeldObjectWriter(object):
    """
    Writes objects to a meld
    """
    def __init__(self, writer, name, metadata):
        """
        Initialized by a MeldWriter
        """
        self.writer = writer
        self.name = name
        self.metadata = metadata

    def write(self, **kwargs):
        """
        Write keyword arguments as fields for the specified object.
        """
        if not self.writer.defined:
            raise MeldNotFinalized("Meld must be finalized before writing data")
        if self.writer._object_header(self.name)[V_INDEX] != V_INDHOLD:
            raise WriteAfterClose("Object "+self.name+" is closed for writing")

        (base, blen) = self.writer._write_object(kwargs)
        self.writer._object_header(self.name)[V_INDEX] = long(base)
        self.writer._object_header(self.name)[V_LENGTH] = long(blen)

class MeldReader(object):
    """
    This class is used for reading melds
    """
    def __init__(self, file, verbose=False, inmemory=False):
        """
        Reads the header information (two reads, one for size and one
        for rest of header).
        """

        # If a file name is passed, open the file...in *binary* mode
        if type(file)==str:
            fp = open(file, "rb")
        else:
            fp = file # Assume this is a file

        # Note, the inmemory option didn't seem to make any difference
        # in my basic benchmarks
        if inmemory:
            import StringIO
            self.fp = StringIO.StringIO(fp.read())
        else:
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

    def asJSON(self, fp):
        """
        This function outputs the wall file in a JSON like
        format (to conform to the format discussed in the
        documentation)
        """
        import json
        json.dump(self.header, fp, indent=4)
        for tabname in self.tables():
            tab = self.read_table(tabname)
            render = {}
            render["name"] = tabname
            render["metadata"] = tab.metadata
            render["vmetadata"] = tab.var_metadata
            sigs = {}
            for sig in tab.signals():
                sigs[sig] = tab.data(sig)
            render["signals"] = sigs
            json.dump(render, fp, indent=4)
        for objname in self.objects():
            obj = self.read_object(objname)
            render = {}
            render["name"] = objname
            render["fields"] = obj.data
            render["metadata"] = obj.metadata
            json.dump(render, fp, indent=4)

    def report(self):
        """
        A little helper routine to write out some basic information about
        how size is allocated.  Not polished and not really for public
        consumption (and not totally accurate or helpful either).
        """
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
        """
        List of tables in this meld
        """
        return self.header[H_TABLES].keys()

    def objects(self):
        """
        List of objects in this meld
        """
        return self.header[H_OBJECTS].keys()
    def read_table(self, table):
        """
        Reads a table from the meld
        """
        if not table in self.tables():
            raise NameError("No table named "+table+" found");
        return MeldTableReader(self, table)

    def read_object(self, objname):
        """
        Reads an object from the meld
        """
        if not objname in self.objects():
            raise NameError("No object named "+table+" found");
        ind = self.header[H_OBJECTS][objname][V_INDEX]
        blen = self.header[H_OBJECTS][objname][V_LENGTH]
        metadata = self.header[H_OBJECTS][objname][O_METADATA]
        self.fp.seek(ind)
        data = self.ser.decode_obj(self.fp, blen)
        return MeldObjectReader(data, metadata)

class MeldTableReader(object):
    """
    Class for reading tables inside a meld
    """
    def __init__(self, reader, table):
        """
        Initialized by MeldReader
        """
        self.reader = reader
        self.table = table
        self.indices = self.reader.header[H_TABLES][table][T_INDICES]
        self.signames = self.reader.header[H_TABLES][table][T_VARIABLES]
        self.metadata = self.reader.header[H_TABLES][table][T_METADATA]
        self.var_metadata = self.reader.header[H_TABLES][table][T_VMETADATA]


    def signals(self):
        """
        Signals in this table.
        """
        return self.signames

    def data(self, signal):
        """
        Data (in this table) associated with a specific signal name
        """
        if not signal in self.indices:
            raise NameError("No signal named "+str(signal)+\
                                " found in table "+str(self.table))
        ind = self.indices[signal][V_INDEX]
        blen = self.indices[signal][V_LENGTH]
        trans = parse_transform(self.indices[signal].get(V_TRANS, None))
        self.reader.fp.seek(ind)
        data = self.reader.ser.decode_vec(self.reader.fp, blen)

        if trans==None:
            return data
        else:
            return trans.apply(data)

class MeldObjectReader(object):
    """
    Class for reading objects from a meld
    """
    def __init__(self, data, metadata):
        self.data = data
        self.metadata = metadata
