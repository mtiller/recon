class MeldWriter(object):
    def __init__(self, fp, verbose=False):
        self.fp = fp
        self.verbose = verbose
    def _check_name(self, name):
        pass
    def add_table(self, name, signals):
        return MeldTableWriter(self, name, signals)
    def add_object(self, name):
        return MeldObjectWriter(self, name)
    def finalize(self):
        pass
    def close(self):
        pass

class MeldTableWriter(object):
    def __init__(self, writer, name, signals):
        self.writer = writer
        self.name = name
        self.signals = signals
    def add_alias(self, alias, of, scale=1.0, offset=0.0):
        pass
    def write(self, name, data):
        pass
    def close(self):
        pass

class MeldObjectWriter(object):
    def __init__(self, writer, name):
        self.writer = writer
        self.name = name
    def write(self, **kwargs):
        pass

class MeldReader(object):
    def __init__(self, fp, verbose=False):
        pass
