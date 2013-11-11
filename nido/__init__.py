from bson import BSON

# This is a unique ID that every wall file starts with so
# it can be identified/verified.
WALL_ID = "nido:wall"

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
    def __init__(self, writer, name, signals):
        self.writer = writer
        self.signals = signals
        self.aliases = {}
        self.name = name
    def add_alias(self, alias, of, scale=1.0, offset=0.0):
        if self.writer.defined:
            raise FinalizedWall()
        if alias in self.aliases:
            raise KeyError("Alias "+alias+" already defined for table "+name)
        self.aliases[alias] = {"of": of, "scale": scale, "offset": offset}
    def add_row(self, *args, **kwargs):
        if len(args)!=0 and len(kwargs)!=0:
            raise ValueError("add_row must be called with either positional or keyword args")
        if len(args)==0:
            aset = set(kwargs.keys())
            cset = set(self.signals)
            if len(aset-cset)>0:
                raise KeyError("Values provided for undefined columns: "+(aset-cset))
            if len(cset-aset)>0:
                raise KeyError("Missing values for columns: "+(cset-aset))
            row = map(lambda x: kwargs[x], self.signals)
            self.writer._add_row(self.name, row)
        else:
            if len(args)!=len(self.signals):
                raise ValueError("Expected %d values, got %d" % (len(self.signals),
                                                                 len(args)))
            self.writer._add_row(self.name, args)

class WallObjectWriter(object):
    def __init__(self, writer, name):
        self.writer = writer
        self.name = name
    def add_field(self, key, value):
        self.writer._add_field(self.name, key, value)

class WallReader(object):
    def __init__(self, fp, verbose=False):
        self.fp = fp
        self.verbose = verbose
        id = self.fp.read(len(WALL_ID))
        if id!=WALL_ID:
            raise IOError("Invalid format: File is not a wall file")
        self.header = self._read()
        if self.verbose:
            print "header = "+str(self.header)
        self.start = fp.tell()
    def _read(self):
        lsb = self.fp.read(1);
        if len(lsb)==0:
            return None
        msb = self.fp.read(1);
        if len(msb)==0:
            raise IOError("Premature EOF");
        l = ord(msb)*256+ord(lsb)
        data = self.fp.read(l-2);
        if len(data)<l-2:
            raise IOError("Premature EOF");
        data = lsb+msb+data
        return BSON(data).decode()
    def objects(self):
        return self.header["objects"]
    def tables(self):
        return self.header["tables"]
    def _read_entries(self, name):
        ret = []
        self.fp.seek(self.start)
        row = self._read()
        while row!=None:
            if self.verbose:
                print "row = "+str(row)
            if name in row:
                ret.append(row[name])
            row = self._read()
        return ret
    def read_object(self, name):
        ret = {}
        if not name in self.header["objects"]:
            raise KeyError("No object named "+name+" present, options are: %s" % \
                           (str(self.header["objects"]),))
        for ent in self._read_entries(name):
            ret[ent[0]] = ent[1]
        return ret
    def read_table(self, name):
        if not name in self.header["tables"]:
            raise KeyError("No table named "+name+" present, options are: %s" % \
                           (str(self.header["tables"]),))
        ret = self.header["tables"][name]
        signals = ret["signals"]
        data = {}
        for signal in signals:
            data[signal] = []
        n = len(signals)
        for ent in self._read_entries(name):
            for index in range(0,n):
                data[signals[index]].append(ent[index])
        ret["data"] = data
        if self.verbose:
            print "table = "+str(ret)
        return ret
