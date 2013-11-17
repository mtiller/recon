from bson import BSON

from util import _read, _read_nolen

# This is a unique ID that every wall file starts with so
# it can be identified/verified.
WALL_ID = "recon:wall:v1"

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
    This class is responsible for writing wall files.  It provides a largely
    pythonic API for doing so.
    """

    def __init__(self, fp, verbose=False):
        """
        Constructor for a WallWriter.  The file-like object fp only needs to
        support the 'write' method.
        """
        self.fp = fp
        self.verbose = verbose
        self.defined = False
        self.tables = {}
        self.objects = {}
        self.metadata = {}
        self.buffered_rows = []
        self.buffered_fields = []
        self.bson = BSON()

    def _check_name(self, name):
        """
        This checks any new name introduced (for either a table or an object)
        to make sure it is unique across the wall.
        """
        if name in self.tables:
            raise KeyError("Wall already contains a table named "+name)
        if name in self.objects:
            raise KeyError("Wall already contains an object named "+name)

    def add_table(self, name, signals):
        """
        This adds a new table to the wall.  If the wall has been finalized, this
        will generated a FinalizedWall exception.  If the name is already used
        by either a table or object, a KeyError exception will be raised.  Otherwise,
        a WallTableWriter object will be returned by this method that can be used
        to populate the table.
        """
        if self.defined:
            raise FinalizedWall()
        self._check_name(name)
        table = WallTableWriter(self, name, signals)
        self.tables[name] = table
        return table

    def add_object(self, name):
        """
        This adds a new object to the wall.  If the wall has been finalized, this
        will generated a FinalizedWall exception.  If the name is already used
        by either a table or object, a KeyError exception will be raised.  Otherwise,
        a WallObjectWriter object will be returned by this method that can be used
        to populate the fields of the object.
        """
        if self.defined:
            raise FinalizedWall()
        self._check_name(name)
        obj = WallObjectWriter(self, name)
        self.objects[name] = obj
        return obj

    def _add_row(self, name, row):
        """
        This is an internal method called by the WallTableWriter object to add
        a new row to the wall.
        """
        if not self.defined:
            raise NotFinalized("Must finalize the wall before adding rows")
        self.buffered_rows.append((name, row))

    def _add_field(self, name, key, value):
        """
        This is an internal method called by the WallObjectWriter object to add
        a new field value to the wall.
        """
        if not self.defined:
            raise NotFinalized("Must finalize the wall before adding fields")
        self.buffered_fields.append((name, key, value))

    def finalize(self):
        """
        This method is called when all tables and objects have been defined.  Once
        called, it is not possible to add new tables or objects.  Furthermore, it
        is not possible to add rows or fields until the wall has been finalized.
        """
        tables = {}
        objects = []
        if self.verbose:
            print "Tables:"
        for table in self.tables:
            tables[table] = {"signals": self.tables[table].signals,
                             "aliases": self.tables[table].aliases}
            if self.verbose:
                print table
                print "Columns: "+str(self.tables[table].signals)
                print "Aliases: "+str(self.tables[table].aliases)
        if self.verbose:
            print "Objects:"
        for obj in self.objects:
            objects.append(obj)
            if self.verbose:
                print obj
        header = {"tables": tables, "objects": objects}
        bhead = self.bson.encode(header)
        if self.verbose:
            print "Header = "+str(header)
            print "String header length: "+str(len(str(header)))
            print "Binary header length: "+str(len(bhead))
            print "Binary header: "+repr(bhead)
        self.fp.write(WALL_ID)
        self.fp.write(bhead)
        self.defined = True

    def flush(self):
        """
        This flushes any pending rows of fields.
        """
        for row in self.buffered_rows:
            if self.verbose:
                print row
            self.fp.write(self.bson.encode({row[0]: row[1]}))
        for field in self.buffered_fields:
            if self.verbose:
                print field
            self.fp.write(self.bson.encode({field[0]: field[1:]}))
        self.buffered_rows = []
        self.buffered_fields = []

class WallTableWriter(object):
    """
    This class is used to add rows to a given wall.
    """
    def __init__(self, writer, name, signals):
        """
        This constructor is only called by the WallWriter class.
        """
        self.writer = writer
        self.signals = signals
        self.aliases = {}
        self.metadata = {}
        self.var_metadata = {}
        self.name = name

    def set_var_metadata(self, name, **kwargs):
        if not name in self.signals and not name in self.aliases:
            raise NameError("No such signal: "+name);
        if not name in self.var_metadata:
            self.var_metadata[name] = {}

        self.var_metadata[name].update(kwargs)

    def add_alias(self, alias, of, scale=1.0, offset=0.0):
        """
        Defines an alias associated with a specific table.  The arguments are
        the name of the alias, the variable it is an alias of (cannot be an
        alias itself), the scale factor and the offset value between the alias
        and base variable.  The value of the alias variable will be computed by
        multiplying the base variable by the scale factor and then adding the
        offset value.
        """
        if self.writer.defined:
            raise FinalizedWall()
        if alias in self.aliases:
            raise KeyError("Alias "+alias+" already defined for table "+name)
        if alias in self.signals:
            raise KeyError("'"+alias+"' is already the name of a signal, cannot be an alias")
        self.aliases[alias] = {"of": of, "scale": scale, "offset": offset}
    def add_row(self, *args, **kwargs):
        """
        This method transforms its arguments (in either positional or keyword form) into
        a row which it then passes back to the TableWriter object to be buffered.
        """
        if len(args)!=0 and len(kwargs)!=0:
            raise ValueError("add_row must be called with either positional or keyword args")
        if len(args)==0:
            # If they specified keyword arguments, make sure they line up exactly with the
            # existing signals.
            aset = set(kwargs.keys())
            cset = set(self.signals)
            if len(aset-cset)>0:
                raise KeyError("Values provided for undefined columns: "+(aset-cset))
            if len(cset-aset)>0:
                raise KeyError("Missing values for columns: "+(cset-aset))
            row = map(lambda x: kwargs[x], self.signals)
            self.writer._add_row(self.name, row)
        else:
            # For positional arguments, just make sure we have the correct number.
            # TODO: Type check...once we have types
            if len(args)!=len(self.signals):
                raise ValueError("Expected %d values, got %d" % (len(self.signals),
                                                                 len(args)))
            self.writer._add_row(self.name, args)

class WallObjectWriter(object):
    """
    This class is used to write object fields back to a wall.
    """
    def __init__(self, writer, name):
        """
        This constructor is only called by the TableWriter class.
        """
        self.writer = writer
        self.name = name
    def add_field(self, key, value):
        """
        This calls the TableWriter and instructs it to add a field.
        """
        self.writer._add_field(self.name, key, value)

class WallReader(object):
    """
    This class is used to read a wall file.
    """
    def __init__(self, fp, verbose=False):
        """
        This is the constructor for the wall reader.  The file like 'fp' object
        must support the read, tell and seek methods.
        """
        self.fp = fp
        self.verbose = verbose
        # Read the first few bytes to make sure they contain the expected
        # string.
        id = self.fp.read(len(WALL_ID))
        if id!=WALL_ID:
            raise IOError("Invalid format: File is not a wall file ("+id+")")
        # Now read the header object
        self.header = _read_nolen(self.fp, self.verbose)
        if self.verbose:
            print "header = "+str(self.header)
        # Record where the end of the header is
        self.start = fp.tell()

    def objects(self):
        """
        Returns the set of objects in this file.
        """
        return self.header["objects"]

    def tables(self):
        """
        Returns the set of tabls in this file.
        """
        return self.header["tables"]

    def _read_entries(self, name):
        """
        Since this file format is journaled, this internal method is used to sweep
        through entries and find the ones that match the named entity.  Only the
        matching objects are retained and returned to the caller for processing.
        """
        ret = []
        # Position the file just after the header
        self.fp.seek(self.start)
        # Read the next BSON document
        row = _read_nolen(self.fp, self.verbose)
        while row!=None:
            if self.verbose:
                print "row = "+str(row)
            # All entries have a single key which is the name of the entity
            # (table or object) that they apply to.  So this basically extracts
            # the value from each entity and appends it to an array which will
            # be returned.
            if name in row:
                ret.append(row[name])
            row = _read_nolen(self.fp, self.verbose)
        return ret

    def read_object(self, name):
        """
        This method extracts the named object.
        """
        ret = {}
        if not name in self.header["objects"]:
            raise KeyError("No object named "+name+" present, options are: %s" % \
                           (str(self.header["objects"]),))
        for ent in self._read_entries(name):
            ret[ent[0]] = ent[1]
        return ret

    def read_table(self, name):
        """
        This method extracts the named table
        """
        if not name in self.header["tables"]:
            raise KeyError("No table named "+name+" present, options are: %s" % \
                           (str(self.header["tables"]),))
        return WallTableReader(self, name, self.header["tables"][name])

class WallTableReader(object):
    def __init__(self, reader, name, header):
        self.reader = reader
        self.name = name
        self.header = header
    def signals(self):
        return self.header["signals"]
    def aliases(self):
        return self.header["aliases"].keys()
    def variables(self):
        return self.signals()+self.aliases()
    def data(self, name):
        if name in self.header["signals"]:
            signal = name
            scale = 1.0
            offset = 0.0
        elif name in self.header["aliases"]:
            signal = self.header["aliases"][name]["of"]
            scale = self.header["aliases"][name]["scale"]
            offset = self.header["aliases"][name]["offset"]
        else:
            raise NameError("No signal or alias named "+name)
        ret = []
        index = self.header["signals"].index(signal)
        for ent in self.reader._read_entries(self.name):
            ret.append(ent[index])
        return ret
