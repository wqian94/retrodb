""" Library for RType numbers. """

import db.rtype as rtype


# General numeric type class
class Numeric(rtype.RType): pass


# Concrete subclasses
class Float(Numeric):
  @classmethod
  def default(cls):
    return 0.0


class Int(Numeric):
  @classmethod
  def default(cls):
    return 0
