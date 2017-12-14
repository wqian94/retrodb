""" Library for predicates during queries. """

import abc

import db.rtype as rtype


# General superclass for all predicates
class Predicate(rtype.RType):
  @abc.abstractmethod
  def before_query(self):
    """
    Function to run at the beginning of a query chain.

    Useful for aggregations that have things to reset, e.g. counters.
    """
    pass

  def default(self):
    """ Predicates by default return False. """
    return False

  @abc.abstractmethod
  def on_record(self, record):
    """
    Function to run on the given record.

    Args:
      record (rrecord.Record): the record to process according to the
        predicate

    Returns a truthy value if the record is to be included (satisfies the
    predicate), or a falsy value if not.
    """
    pass


# Concrete instances of predicates
class Where(Predicate):
  def __init__(self, func):
    """
    Uses func to evaluate each record to determine satisfaction.

    In short, acts like the filter class.

    Useful for examples like Where(lambda record: record['key'] % 2) for
    filtering results.

    Args:
      func (callable): the predicate function.
    """
    self._func = func

  def before_query(self): pass

  def on_record(self, record):
    return self._func(record)
