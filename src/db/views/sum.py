""" Library for retroactively-updated SUM view. """

import db.rview as rview
import db.types.numbers as rnum


class Sum(rview.RViewIntegrable):
  def __init__(self, obj, field):
    """
    Creates a retroactively-updated SUM view for the given obj on the given
    field.

    Initializes the value to rnum.Int.default().

    Args:
      obj (RView | RTable): the object for which to compute the sum
      field (str): the name of the field on which to compute the sum
    """
    super(Sum, self).__init__(obj)
    self._field = field
    self._value = rnum.Int.default()

  def _callback_delete(self, change):
    self._value -= change.record[self._field]

  def _callback_insert(self, change):
    self._value += change.record[self._field]
