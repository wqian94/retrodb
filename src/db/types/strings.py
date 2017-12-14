""" Library for RType strings. """

import db.rtype as rtype


class String(rtype.RType):
  @classmethod
  def default(cls):
    return ''
