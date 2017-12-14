""" Library for RType numbers. """

import db.rtype as rtype


class Float(rtype.RType):
  @classmethod
  def default(cls):
    return 0.0


class Int(rtype.RType):
  @classmethod
  def default(cls):
    return 0
