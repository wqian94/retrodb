""" Library for error classes. """


class RTableInitException(Exception): pass
class RTableInvalidFieldException(Exception): pass
class RViewInitException(Exception): pass
class RViewValueUninitializedException(Exception): pass
