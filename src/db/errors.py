""" Library for error classes. """


# General Exception superclass
class RException(Exception): pass


# More concrete subclasses
class RTableInitException(RException): pass
class RTableInvalidFieldException(RException): pass
class RTypeIncompatibleException(RException): pass
class RViewInitException(RException): pass
class RViewValueUninitializedException(RException): pass
