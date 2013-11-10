class FinalizedWall(Exception):
    pass

class ColumnMismatch(Exception):
    pass

class WallWriter(object):
    def __init__(self, fp):
        self.fp = fp
        self.defined = False
        self.tables = {}
        self.objects = {}
        self.buffered_rows = []
        self.buffered_fields = []
    def add_table(self, name, signals):
        if self.defined:
            raise FinalizedWall()
        if name in self.tables:
            raise KeyError("Wall already contains table named "+name)
        table = WallTableWriter(self, name, signals)
        self.tables[name] = table
        return table
    def add_object(self, name):
        if self.defined:
            raise FinalizedWall()
        if name in self.objects:
            raise KeyError("Wall already contains object named "+name)
        obj = WallObjectWriter(self, name)
        self.objects[name] = obj
        return obj
    def _add_row(self, name, row):
        self.buffered_rows.append((name, row))
    def _add_field(self, name, key, value):
        self.buffered_fields.append((name, key, value))
    def finalize(self):
        print "Tables:"
        for table in self.tables:
            print table
            print "Columns: "+str(self.tables[table].signals)
            print "Aliases: "+str(self.tables[table].aliases)
        print "Objects:"
        for obj in self.objects:
            print obj
        self.defined = True
    def flush(self):
        for row in self.buffered_rows:
            print row
        for field in self.buffered_fields:
            print field
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
    def add_row(self, **kwargs):
        aset = set(kwargs.keys())
        cset = set(self.signals)
        if len(aset-cset)>0:
            raise KeyError("Values provided for undefined columns: "+(aset-cset))
        if len(cset-aset)>0:
            raise KeyError("Missing values for columns: "+(cset-aset))
        row = map(lambda x: kwargs[x], self.signals)
        self.writer._add_row(self.name, row)

class WallObjectWriter(object):
    def __init__(self, writer, name):
        self.writer = writer
        self.name = name
    def add_field(self, key, value):
        self.writer._add_field(self.name, key, value)
