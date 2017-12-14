""" Library for retroactively-updated SUM view. """

import db.rrecord as rrecord
import db.rview as rview
import db.types.numbers as rnum


class Sum(rview.RViewIntegrable):
  def __init__(self, obj, time, field):
    """
    Creates a retroactively-updated SUM view for the given obj on the given
    field.

    The schema type of the field must be a Numeric type.

    Initializes the value to rnum.Int.default().

    Args:
      obj (Subscribable): the object for which to compute the sum
      time (int): the time at which the sum is to be computed
      field (str): the name of the field on which to compute the sum

    Raises a RTypeIncompatibleException if field is not a Numeric type.
    """
    if not issubclass(obj.schema[field], rnum.Numeric):
      raise errors.RTypeIncompatibleException(
        'Field %s is a %s, not a Numeric RType' % (
        field, obj.schema[field].__name__))

    super(Sum, self).__init__(obj)
    self._field = field
    self._time = time

    vfield = 'SUM(%s)' % field  # Name of the view's field
    self._schema = {vfield: obj.schema[field]}

    self._record = None
    self._update(rnum.Int.default())

    # Start callback thread
    self._start()

  def _all_records(self):
    return (self._record,)

  def _callback_delete(self, record):
    if record.time <= self._time:
      self._update(self._value - record[self._field])

  def _callback_erase(self, time, records):
    if time <= self._time:
      value = self._value
      for record in records:
        if rrecord.Record.INSERT == record.action:
          value -= record[self._field]
        elif rrecord.Record.DELETE == record.action:
          value -= record[self._field]
      self._update(value)

  def _callback_insert(self, record):
    if record.time <= self._time:
      self._update(self._value + record[self._field])

  def _update(self, value):
    """
    Updates the view to be of the given value, using delete-insert.

    Includes updates for pubsub, etc.

    Args:
      value (int | float): the new value of the view
    """
    if self._record:
      self._add_to_history(self._record.delete(self._time))
    self._value = value
    record = rrecord.Record(
      self, self._time, rrecord.Record.INSERT,
      **{tuple(self.schema.keys())[0]: value})
    self._record = record
    self._add_to_history(record)

  @property
  def schema(self):
    """ Schema of the view. """
    return self._schema

  @property
  def time(self):
    """ Time of the view's computation. """
    return self._time
