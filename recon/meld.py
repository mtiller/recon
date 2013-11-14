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
        self.defined = False
        self.tables = {}
        self.objects = {}
        self.cur = None # Current object being written

    def _check_names(self, name):
        """
        This checks any new name introduced (for either a table or an object)
        to make sure it is unique across the wall.
        """
        if name in self.tables:
            raise KeyError("Wall already contains a table named "+name)
        if name in self.objects:
            raise KeyError("Wall already contains an object named "+name)

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
        self.objects[obj] = obj
        self.cur = obj
        return obj

    def close(self):
        if not self.defined:
            self.finalize()
        pass

class MeldTableWriter(object):
    def __init__(self, writer, name, signals):
        self.writer = writer
        self.name = name
        self.signals = signals
    def add_alias(self, alias, of, scale=1.0, offset=0.0):
        pass
    def write(self, name, data):
        if self.writer.cur != self:
            raise FinalizedTable("Table "+name+" is already closed for writing")
        pass
    def close(self):
        pass

class MeldObjectWriter(object):
    def __init__(self, writer, name):
        self.writer = writer
        self.name = name
    def write(self, **kwargs):
        if self.writer.cur != self:
            raise FinalizedTable("Table "+name+" is already closed for writing")
        pass
    def close(self):
        pass

class MeldReader(object):
    def __init__(self, fp, verbose=False):
        pass
