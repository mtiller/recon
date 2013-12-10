from serial import BSONSerializer, MsgPackSerializer

from util import write_len, read_len, parse_transform

# This is a unique ID that every wall file starts with so
# it can be identified/verified.
WALL_ID = "recon:wall:v01"

#DEFSER = BSONSerializer
DEFSER = MsgPackSerializer

# Header
H_METADATA = "fmeta"
H_TABLES = "tabs"
H_OBJECTS = "objs"

# Table
T_METADATA = "tmeta"
T_VMETADATA = "vmeta"
T_SIGNALS = "sigs"
T_ALIASES = "als"

# Aliases
A_OF = "s"
V_TRANS = "t"


class FinalizedWall(Exception):
    """
    Thrown when an attempt is made to change structural definitions
    of a wall file after it has been finalized."
    """
    pass

class NotFinalized(Exception):
    """
    This exception is thrown if an attempt is made to write to the wall
    before it is finalized.
    """

class WallWriter(object):
    """
    This class is responsible for writing wall files.  It provides a
    largely pythonic API for doing so.
    """

    def __init__(self, fp, metadata={}, verbose=False):
        """
        Constructor for a WallWriter.  The file-like object fp only
        needs to support the 'write' method.

        Note: all metadata must be supplied at the time the wall
        file is created.
        """
        self.fp = fp
        self.verbose = verbose
        self.defined = False
        self.tables = {}
        self.objects = {}
        self._metadata = metadata
        self.buffered_rows = []
        self.buffered_fields = [] # [{objname -> {field -> value}}]
        self.ser = DEFSER()

    def _check_name(self, name):
        """
        This checks any new name introduced (for either a table or an
        object) to make sure it is unique across the wall.
        """
        if name in self.tables:
            raise KeyError("Wall already contains a table named "+name)
        if name in self.objects:
            raise KeyError("Wall already contains an object named "+name)

    def add_table(self, name, metadata=None):
        """
        This adds a new table to the wall.  If the wall has been
        finalized, this will generated a FinalizedWall exception.  If
        the name is already used by either a table or object, a
        KeyError exception will be raised.  Otherwise, a
        WallTableWriter object will be returned by this method that
        can be used to populate the table.

        Note: All metadata must be supplied at the time when the table
        is created.
        """
        if self.defined:
            raise FinalizedWall()
        self._check_name(name)
        table = WallTableWriter(self, name, metadata)
        self.tables[name] = table
        return table

    def add_object(self, name, metadata=None):
        """
        This adds a new object to the wall.  If the wall has been
        finalized, this will generated a FinalizedWall exception.  If
        the name is already used by either a table or object, a
        KeyError exception will be raised.  Otherwise, a
        WallObjectWriter object will be returned by this method that
        can be used to populate the fields of the object.

        Note: All metadata must be supplied at the time the object
        is created.
        """
        if self.defined:
            raise FinalizedWall()
        self._check_name(name)
        obj = WallObjectWriter(self, name, metadata)
        self.objects[name] = obj
        return obj

    def _add_row(self, name, row):
        """
        This is an internal method called by the WallTableWriter
        object to add a new row to the wall.
        """
        if not self.defined:
            raise NotFinalized("Must finalize the wall before adding rows")
        self.buffered_rows.append((name, row))

    def _add_fields(self, name, kwargs):
        """
        This is an internal method called by the WallObjectWriter
        object to add a new field value to the wall.
        """
        if not self.defined:
            raise NotFinalized("Must finalize the wall before adding fields")
        self.buffered_fields.append((name, kwargs))

    def finalize(self):
        """
        This method is called when all tables and objects have been
        defined.  Once called, it is not possible to add new tables or
        objects.  Furthermore, it is not possible to add rows or
        fields until the wall has been finalized.
        """
        tables = {}
        objects = {}
        if self.verbose:
            print "Tables:"
        for table in self.tables:
            tables[table] = {T_SIGNALS: self.tables[table].signals,
                             T_ALIASES: self.tables[table].aliases,
                             T_METADATA: self.tables[table]._metadata,
                             T_VMETADATA: self.tables[table]._vmd}
            if self.verbose:
                print table
                print "Columns: "+str(self.tables[table].signals)
                print "Aliases: "+str(self.tables[table].aliases)
                print "Metadata: "+str(self.tables[table]._metadata)
                print "Var Metadata: "+str(self.tables[table]._vmd)
        if self.verbose:
            print "Objects:"
        for obj in self.objects:
            if self.objects[obj]._metadata!=None:
                objects[obj] = self.objects[obj]._metadata
            else:
                objects[obj] = {}
            if self.verbose:
                print obj
        header = {H_TABLES: tables,
                  H_OBJECTS: objects,
                  H_METADATA: self._metadata}
        bhead = self.ser.encode_obj(header)
        if self.verbose:
            print "Header = "+str(header)
            print "String header length: "+str(len(str(header)))
            print "Binary header length: "+str(len(bhead))
            print "Binary header: "+repr(bhead)
        self.fp.write(WALL_ID)
        write_len(self.fp, len(bhead))
        self.fp.write(bhead)
        self.defined = True

    def flush(self):
        """
        This flushes any pending rows of fields.
        """
        for row in self.buffered_rows:
            if self.verbose:
                print row
            rowdata = self.ser.encode_obj({row[0]: row[1]})
            write_len(self.fp, len(rowdata))
            self.fp.write(rowdata)
        for field in self.buffered_fields:
            if self.verbose:
                print field
            fielddata = self.ser.encode_obj({field[0]: field[1]})
            write_len(self.fp, len(fielddata))
            self.fp.write(fielddata)
        self.buffered_rows = []
        self.buffered_fields = []

class WallTableWriter(object):
    """
    This class is used to add rows to a given wall.
    """
    def __init__(self, writer, name, metadata):
        """
        This constructor is only called by the WallWriter class.
        """
        self.writer = writer
        self.signals = []
        self.aliases = {}
        self._metadata = metadata
        self._vmd = {}
        self._vtypes = {}
        self.name = name

    def _check_name(self, name):
        if name in self.aliases:
            raise KeyError("'"+name+"' is already the name of an alias in table "+self.name)
        if name in self.signals:
            raise KeyError("'"+name+"' is already the name of a signal in table "+self.name)

    def add_signal(self, signal, metadata=None, vtype=None):
        """
        Used to add a signal to a table.

        Note: All metadata must be supplied at the time the signal
        is added.
        """
        self._check_name(signal)
        self.signals.append(signal)
        if metadata!=None:
            self._vmd[signal]=metadata
        if vtype!=None:
            if type(vtype)!=type:
                raise TypeError("Type specifier '"+str(vtype)+"' is not a type")
            self._vtypes[signal] = vtype

    def add_alias(self, alias, of, transform=None, metadata=None):
        """
        Defines an alias associated with a specific table.  The
        arguments are the name of the alias, the variable it is an
        alias of (cannot be an alias itself), the transform variable
        defines the transform between the alias and base variable.
        The value of the alias variable will be computed by
        applying the transform (as defined in the specification).

        Note: All metadata must be supplied at the time the alias
        is added.

        """
        if self.writer.defined:
            raise FinalizedWall()
        self._check_name(alias)

        self.aliases[alias] = {A_OF: of}
        if transform!=None:
            if parse_transform(transform)==None:
                raise ValueError("Transform '"+str(transform)+"' could not be parsed")
            self.aliases[alias][V_TRANS] = transform
        if metadata!=None:
            self._vmd[alias]=metadata

    def add_row(self, *args, **kwargs):
        """
        This method transforms its arguments (in either positional or
        keyword form) into a row which it then passes back to the
        TableWriter object to be buffered.
        """
        if len(args)!=0 and len(kwargs)!=0:
            raise ValueError("add_row must be called with either"+\
                                 " positional or keyword args")
        if len(args)==0:
            # If they specified keyword arguments, make sure they line
            # up exactly with the existing signals.
            aset = set(kwargs.keys())
            cset = set(self.signals)
            if len(aset-cset)>0:
                raise KeyError("Values provided for undefined columns: "+\
                                   str(aset-cset))
            if len(cset-aset)>0:
                raise KeyError("Missing values for columns: "+(cset-aset))
            row = map(lambda x: kwargs[x], self.signals)

            # Enforce any type constraints
            for signal in self._vtypes:
                if type(kwargs[signal])!=self._vtypes[signal]:
                    raise TypeError("Value of '%s' (%s) doesn't match expected type %s" % \
                                    (signal, str(kwargs[signal]), str(self._vtypes[signal])))
            self.writer._add_row(self.name, row)
        else:
            # For positional arguments, just make sure we have the
            # correct number.

            if len(args)!=len(self.signals):
                raise ValueError("Expected %d values, got %d" % \
                                     (len(self.signals), len(args)))
            
            for idx in range(0,len(self.signals)):
                signal = self.signals[idx]
                if signal in self._vtypes:
                    val = args[idx]
                    if type(val)!=self._vtypes[signal]:
                        raise TypeError("Value of '%s' (%s) doesn't match expected type %s" % \
                                        (signal, str(val), str(self._vtypes[signal])))

            self.writer._add_row(self.name, args)

class WallObjectWriter(object):
    """
    This class is used to write object fields back to a wall.
    """
    def __init__(self, writer, name, metadata):
        """
        This constructor is only called by the TableWriter class.
        """
        self.writer = writer
        self.name = name
        self._metadata = metadata
    def add_fields(self, **kwargs):
        """
        This calls the TableWriter and instructs it to add a field.
        """
        self.writer._add_fields(self.name, kwargs)

class WallReader(object):
    """
    This class is used to read a wall file.
    """
    def __init__(self, fp, verbose=False):
        """
        This is the constructor for the wall reader.  The file like
        'fp' object must support the read, tell and seek methods.
        """
        self.fp = fp
        self.verbose = verbose

        self.ser = DEFSER()

        # Read the first few bytes to make sure they contain the expected
        # string.
        id = self.fp.read(len(WALL_ID))
        if id!=WALL_ID:
            raise IOError("Invalid format: File is not a wall file ("+id+")")

        # Now read the length of the header object
        # and then the header object itself
        self.headlen = read_len(self.fp, verbose=verbose)
        if verbose:
            print "Header length: "+str(self.headlen)
        self.header = self.ser.decode_obj(self.fp, length=self.headlen,
                                          verbose=verbose)
        self.metadata = self.header[H_METADATA]
        if self.verbose:
            print "header = "+str(self.header)

        # Record where the end of the header is.
        self.start = fp.tell()

    def asJSON(self, fp):
        """
        This function outputs the wall file in a JSON like
        format (to conform to the format discussed in the
        documentation)
        """
        import json
        import struct
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
    def objects(self):
        """
        Returns the set of objects in this file.
        """
        if not H_OBJECTS in self.header:
            print "WARNING: Object information is missing from header"
            return set()
        if self.header[H_OBJECTS]==None:
            print "WARNING: Invalid header value for objects"
            return set()
        return self.header[H_OBJECTS].keys()

    def tables(self):
        """
        Returns the set of tabls in this file.
        """
        return self.header[H_TABLES]

    def _read_entries(self, name):
        """
        Since this file format is journaled, this internal method is
        used to sweep through entries and find the ones that match the
        named entity.  Only the matching objects are retained and
        returned to the caller for processing.
        """
        ret = []

        # Position the file just after the header
        self.fp.seek(self.start)

        # Read the next object
        rowlen = read_len(self.fp, ignoreEOF=True, verbose=self.verbose)
        while rowlen!=None:
            row = self.ser.decode_obj(self.fp, length=rowlen,
                                      verbose=self.verbose)
            if self.verbose:
                print "row = "+str(row)
            # All entries have a single key which is the name of the entity
            # (table or object) that they apply to.  So this basically extracts
            # the value from each entity and appends it to an array which will
            # be returned.
            if name in row:
                ret.append(row[name])
            rowlen = read_len(self.fp, ignoreEOF=True, verbose=self.verbose)
        return ret

    def read_object(self, name):
        """
        This method extracts the named object.
        """
        if not name in self.header[H_OBJECTS]:
            raise KeyError("No object named "+name+ \
                               " present, options are: %s" % \
                               (str(self.header[H_OBJECTS]),))
        return WallObjectReader(self, name, self.header[H_OBJECTS][name])

    def read_table(self, name):
        """
        This method extracts the named table
        """
        if not name in self.header[H_TABLES]:
            raise KeyError("No table named "+name+\
                               " present, options are: %s" % \
                               (str(self.header[H_TABLES]),))
        return WallTableReader(self, name, self.header[H_TABLES][name])

class WallTableReader(object):
    """
    This class is responsible for reading tables in wall files.
    """
    def __init__(self, reader, name, header):
        """
        Initialize this object with information from
        the reader (which is who creates this)
        """
        self.reader = reader
        self.name = name
        self.header = header
        self.metadata = self.header[T_METADATA]
        self.var_metadata = self.header[T_VMETADATA]
    def signals(self):
        """
        Signals in this table
        """
        return self.header[T_SIGNALS]

    def aliases(self):
        """
        Aliases in this table
        """
        return self.header[T_ALIASES].keys()

    def variables(self):
        """
        Variables in this table (signals + aliases)
        """
        return self.signals()+self.aliases()

    def alias_of(self, name):
        """
        Indicate what a given alias is an alias of
        """
        return self.header[T_ALIASES][name][A_OF]

    def alias_transform(self, name):
        """
        Transformation **object** between alias and base signal
        """
        return parse_transform(self.header[T_ALIASES][name].get(V_TRANS, None))

    def alias_transform_string(self, name):
        """
        Transformation **object** between alias and base signal
        """
        return self.header[T_ALIASES][name].get(V_TRANS, None)

    def vmetadata(self, name):
        return self.var_metadata.get(name, None)

    def data(self, name):
        """
        Get the data for a given variable (signal or alias)
        """
        if name in self.header[T_SIGNALS]:
            signal = name
            trans = None
        elif name in self.header[T_ALIASES]:
            signal = self.header[T_ALIASES][name][A_OF]
            trans = self.alias_transform(name)
        else:
            raise NameError("No signal or alias named "+name)
        ret = []
        index = self.header[T_SIGNALS].index(signal)
        ret = map(lambda x: x[index],
                  self.reader._read_entries(self.name))
        if trans==None:
            return ret
        else:
            return trans.apply(ret)
        return ret

class WallObjectReader(object):
    def __init__(self, reader, name, header):
        self.name = name
        self.reader = reader
        self.metadata = header
        self.data = {}
        for ent in self.reader._read_entries(name):
            self.data.update(ent)
