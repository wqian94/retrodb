""" Library for a retroactively-updatable database table. """

import collections
import functools
import threading

import db.errors as errors
import db.rrecord as rrecord
import db.rtype as rtype
import db.types.numbers as rnum
import db.types.subscribable as rpubsub

# Views
import db.views.select as rviewselect
import db.views.sum as rviewsum


class RTable(rpubsub.Subscribable):
  """
  Retroactively-updatable database table implementation.

  Existing features:
    - Multi-column support
    - Retroactive operations
    - Retroactively-updated view generation
  """
  def __init__(self, **schema):
    """
    Initializes a new retroactively-updatable database table, with the given
    schema defining columns and their types.

    Columns are passed in as keyword arguments, in the format field=RType,
    where RType is a subclass of the RType abstract base class.

    Args:
      schema (dict): field=RType format

    Raises a RTableInitException if a column's type isn't a subclass of RType.
    """
    super(RTable, self).__init__()
    self._schema = schema
    for coltype in schema.values():
      if not issubclass(coltype, rtype.RType):
        raise errors.RTableInitException('%s is not a RType' % coltype)

    self._records = collections.defaultdict(tuple)  # Keyed on time
    self._record_lock = threading.RLock()

  def __repr__(self):
    """ Returns the string representation of this table. """
    schema_str = ', '.join(
      '\'%s\'=%s' % (k, v) for k, v in self._schema.items())
    return '%d=RTable(%s)' % (id(self), schema_str)

  def __str__(self):
    """ Returns the stringification of this table. """
    schema_string = ', '.join(
      '%s=%s' % (k, v.__name__) for k, v in self._schema.items())
    records_string = "\n\t".join(
      str(r) for records in self._records.values() for r in records)
    return 'RTable{\n\t[Schema: %s]\n\t%s\n}' % (schema_string, records_string)

  def _all_records(self):
    with self._record_lock:
      return functools.reduce(
        lambda allrecords, some: allrecords + some, self._records.values(), ())

  def delete(self, time, record):
    """
    Deletes, at the given time, the provided record. The history of the record
    remains in history.

    Args:
      time (int): the time at which we want to delete the record
      record (rrecord.Record): the record to delete

    Returns a reference to the record of the deletion.
    """
    with self._record_lock:
      drecord = record.delete(time)
      self._records[time] += (drecord,)
      self._add_to_history(drecord)
      return drecord

  def erase(self, time):
    """
    Erases, from all history, the record(s) at the given time. Unlike delete(),
    this will not generate a deletion record, but will instead remove the
    existence of the records.

    Args:
      time (int): the time at which we want to erase the record

    Returns a tuple of the erased records.
    """
    with self._record_lock:
      erased = self._records[time]
      del self._records[time]

      # Break inversion references
      for record in erased:
        if record._inversion and (record._inversion._inversion == record):
          record._inversion._inversion = None
      self._remove_from_history(*erased)
      return erased

  def insert(self, time, **values):
    """
    Inserts, at the given time, a record with the given field:value pairs. Any
    fields in the schema but not given a value will be filled automagically
    with the default value for that type.

    Args:
      time (int): the time at which we want to insert the record
      values (dict): the field:value associations, parameterized as keyword
        arguments. Any fields in the schema but not passed in will be filled in
        using the default values of their types.

    Returns a reference to the associated record that was created.

    Raises a RTableInvalidFieldException if a field not in the schema is
    specified.
    """
    invalid_fields = values.keys() - self._schema.keys()
    if len(invalid_fields):
      raise errors.RTableInvalidFieldException(
        'These fields are not part of the schema: %s' % \
        ', '.join(invalid_fields))

    record = rrecord.Record(self, time, rrecord.Record.INSERT, **values)
    with self._record_lock:
      self._records[time] += (record,)
      self._add_to_history(record)
      return record

  @property
  def schema(self):
    """ Access the table's schema. """
    return self._schema

  def select(self, time, *fields, predicates=()):
    """
    Retrieves, as a tuple of dicts, the requested fields from the table.

    Args:
      time (int): the time at which the selection is to be made
      fields (tuple(str)): the names of the fields to retrieve
      predicate (tuple(rpred.Predicate)): the predicates to enforce. Defaults
        to ()

    Returns a tuple of dictionary objects, which is a tuple of the records'
    selected fields.
    """
    return rviewselect.Select(self, time, *fields, predicates=predicates)

  def sum(self, time, field):
    """
    Retrieves a sum of the records along the given field.

    The schema type of the field must be a Numeric type.

    Args:
      time (int): the time at which the sum is to be computed
      field (str): the field on which to compute the sum

    Returns a Sum view of those fields in this table.
    """
    return rviewsum.Sum(self, time, field)
