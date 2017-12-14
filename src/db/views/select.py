""" Library for retroactively-updated SELECT view. """

import db.rview as rview


class Select(rview.RView):
  def __init__(self, obj, time, *fields, predicates=()):
    """
    Creates a retroactively-updated SELECT view for the given obj on the given
    fields.

    If given predicates, all predicates must be satisfied in order to be part
    of the select. In other words, predicates are inherently and'ed together.

    Args:
      obj (Subscribable): the object for which to compute the sum
      time (int): the time at which the sum is to be computed
      fields (tuple(str)): the names of the fields to select
      predicate (tuple(rpred.Predicate)): the predicates to enforce. Defaults
        to ()
    """
    super(Select, self).__init__(obj)
    self._fields = fields
    self._predicates = predicates
    self._time = time
    self._value = ()

    self._schema = {field: obj.schema[field] for field in fields}
    self._records = ()

    for predicate in predicates:
      predicate.before_query()

    # Start callback thread
    self._start()

  def _all_records(self):
    return self._records

  def _callback_delete(self, record):
    if record.time <= self._time:
      index = 0
      while index < len(self._records):
        if self._records[index] == record._inversion:
          self._add_to_history(self._records[index].delete(self._time))
          self._records = self._records[:index] + self._records[index + 1:]
          self._value = self._value[:index] + self._value[index + 1:]
          break
        index += 1

  def _callback_erase(self, time, records):
    if time <= self._time:
      index = 0
      while index < len(self._records):
        record = self._records[index]
        if record in records:
          self._records = self._records[:index] + self._records[index + 1:]
          self._value = self._value[:index] + self._value[index + 1:]

          # Deal with duplicate records
          rindex = 0
          while rindex < len(records):
            if record == records[rindex]:
              records = records[:rindex] + records[rindex + 1:]
              break

          self._remove_from_history(record)
        else:
          index += 1

  def _callback_insert(self, record):
    if record.time <= self._time:
      for predicate in self._predicates:
        if not predicate.on_record(record):
          return
      self._records += (record,)
      self._value += ({field: record[field] for field in self._fields},)
      self._add_to_history(record)

  @property
  def schema(self):
    """ Schema of the view. """
    return self._schema

  @property
  def time(self):
    """ Time of the view's computation. """
    return self._time
