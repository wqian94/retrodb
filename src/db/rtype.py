""" Library for types used in retroactive results. """

import abc


class RType(abc.ABC):
  """ Abstract class for different types of values in the database. """
  @classmethod
  @abc.abstractmethod
  def default(cls):
    """
    Returns an instance of the RType with its default value. For example, a
    numeric type might default to 0, and a string type might default to ''.
    """
