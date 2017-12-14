""" Library for records in a database table. """

import enum
import functools


class Record(object):
  """ Represents a given row record in a table. """
  def __init__(self, table, time, action, **values):
    """
    Creates and initializes a new record within the given table, containing the
    given values.

    The values are passed as keyword arguments in field=col_value format. If
    a column is not specified, it is set to the default value specified by the
    type.

    Args:
      table (RTable): the table to which this record belongs
      time (int): the time of this record
      action (
      values (dict): the requested values of the record, in field=col_value
        format
    """
    self._action = action
    self._inversion = None  # Reference to the "opposing" record
    self._table = table
    self._time = time
    self._values = {}
    for field, coltype  in table.schema.items():
      if field in values:
        self._values[field] = values[field]
      else:
        self._values[field] = coltype.default()

  def __eq__(self, other):
    """ Returns whether self and other represent the same record. """
    return (other is not None) and \
      (self._action == other._action) and \
      (self._table == other._table) and \
      (self._time == other._time) and \
      functools.reduce(
        lambda acc, field:
          acc and (self._values[field] == other._values[field]),
        self._table.schema.keys(), True)

  def __getitem__(self, field):
    """
    Retrieves the value in the given field.

    Args:
      field (str): the name of the field

    Returns the value associated with the field.
    """
    return self._values[field]

  def __len__(self):
    """ Returns the number of field:value pairs. """
    return len(self._values)

  def __repr__(self):
    """ Returns the string representation of this record. """
    val_str = ', '.join('%s=%s' % (k, repr(v)) for k, v in self._values.items())
    if len(val_str):
      val_str = ', ' + val_str
    return '%d=Record(RTable=%s, %d, %s%s)' % (
      id(self), id(self._table), self._time, self._action, val_str)

  def __str__(self):
    """ Returns the stringification of this record. """
    return 'Record(%s@time=%d)[%s]' % (
      self._action.name, self._time,
      ', '.join('%s=%s' % (k, repr(v)) for k, v in self._values.items()))

  @property
  def action(self):
    """ Returns this record's action type, e.g. INSERT or DELETE. """
    return self._action

  def delete(self, time):
    """
    Associates this record with a record stating its deletion, and returns
    that record.

    Args:
      time (int): the time at which deletion takes effect
    """
    drecord = Record(self._table, time, Record.DELETE, **self._values)
    self._inversion = drecord
    drecord._inversion = self
    return drecord

  def fields(self):
    """ Returns the set of fields in this record. """
    return self._values.keys()

  @property
  def time(self):
    """ Returns the time of this record taking effect. """
    return self._time


# Enum for the types of record actions changes
RecordAction = enum.Enum('RecordAction', 'INSERT DELETE ERASE')
for updatename, update in RecordAction.__members__.items():
  setattr(Record, updatename, update)
del RecordAction


class Erasure(Record):
  """ Special subclass for representing erasures. """
  def __init__(self, time, *records):
    """
    Generates the Erasure instance for the records, which were added at the
    given time.

    Args:
      time (int): the time at which these records were added
      records (tuple(Record)): the records to erase
    """
    self._action = Record.ERASE
    self._inversion = None
    self._records = records
    self._table = None
    self._time = time
    self._values = dict(zip(range(len(records)), records))

  def delete(self):
    """
    Erasures can't be undone, so calling this raises a
    RErasureUndeletableException.
    """
    raise errors.RErasureUndeletableException(
      'Cannot delete a %s' % self.type.name)

  @property
  def records(self):
    """ Returns the records to be erased. """
    return self._records
