class WallWriter(object):
    def __init__(self, fp):
        self.fp = fp
        self.defined = False
    def add_table(self, name, signals):
        return WallTableWriter(self, name)
    def add_object(self, name):
        return WallObjectWriter(self, name)
    def finalize(self):
        self.defined = True
    def flush(self):
        pass

class WallTableWriter(object):
    def __init__(self, writer, name):
        self.writer = writer
        self.name = name
    def add_alias(self, alias, of, scale=1.0, offset=0.0):
        pass
    def add_row(self, **kwargs):
        pass

class WallObjectWriter(object):
    def __init__(self, writer, name):
        self.writer = writer
        self.name = name
    def add_field(self, key, value):
        pass
