""" Library for a retroactively-updatable database table. """

import collections
import random
import threading

import db.errors as errors
import db.rrecord as rrecord
import db.rtype as rtype


class RTable(object):
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
    self._schema = schema
    for coltype in schema.values():
      if not issubclass(coltype, rtype.RType):
        raise errors.RTableInitException('%s is not a RType' % coltype)

    self._checkpoints = {}  # Checkpoints -> (historical stamps, refcount)
    self._history = []  # Index = checkpoint_stamp - history_epoch
    self._history_epoch = 0  # Current epoch of historical stamps

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

  def _generate_checkpoint(self, checkpoint=None):
    """
    Returns a new checkpoint that isn't in use, preferring the provided
    checkpoint if it is not None.

    Not thread-safe.

    Args:
      checkpoint (int): the preferred checkpoint to use. Defaults to None.

    Returns a new, unused checkpoint.
    """
    while checkpoint in self._checkpoints:
      checkpoint = random.randint(0, (1 << 64) - 1)
    return checkpoint

  def _checkpoint_history(self, checkpoint):
    """
    Returns the latest history element as a (record, checkpoint) pair. If there
    is no latest record to checkpoint, the first element will be None. If there
    is no checkpoint for that element yet, this attempts to use the provided
    checkpoint, or if that is in use, this will create a new checkpoint.

    Is thread-safe.

    Args:
      checkpoint (int): the preferred checkpoint to use

    Returns a (record, checkpoint) pair for the latest history element.      
    """
    with self._record_lock:
      # Only need to create a checkpoint when history does not exist, or there
      # is no existing checkpoint for the latest history element.
      if (not len(self._history)) or (self._history[-1][1] is None):
        # Reaffirm our checkpoint
        checkpoint = self._get_checkpoint(checkpoint=checkpoint)
        if len(self._history):
          self._history[-1][1] = checkpoint
        else:
          self._history[-1] = (None, checkpoint)
        stamp = len(self._history) + self._history_epoch
        self._checkpoint[checkpoint] = (stamp, 1)

      return self._history[-1]

  def delete(self, time, record):
    """
    Deletes, at the given time, the provided record. The history of the record
    remains in history.

    Args:
      time (int): the time at which we want to delete the record
      record (Record): the record to delete

    Returns a reference to the record of the deletion.
    """
    with self._record_lock:
      drecord = record.delete()
      self._records[time] += (drecord,)
      self._history += ((drecord, None),)  # Record, checkpoint

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

      for index in range(len(self._history)):
        if self._history[index] in erased:
          self._history[index] = (None, self._history[index][1])

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
      self._history += ((record, None),)  # Record, checkpoint
      return record

  @property
  def schema(self):
    """ Access the table's schema. """
    return self._schema

  def subscribe(self):
    """
    Returns the current view as a changelist of records.

    If a checkpoint association already exists, then this will use the existing
    association. Otherwise, this will create a new association.
    """
    # Add non-threadsafe checkpoint generation to throw in a bit of time so
    # that calls are more likely to disinterleave.
    checkpoint = self._generate_checkpoint()

    with self._record_lock:
      _, checkpoint = self._checkpoint_history(checkpoint)
      return checkpoint, ((self._records.values(),), ())

  def subscribe_since(self, checkpoint):
    """
    Retrieves the changelist since the given checkpoint, along with a new
    checkpoint.

    Args:
      checkpoint (int): the checkpoint marker at which to start generating the
        changelist

    Returns a new checkpoint and the changelist of the record changes since the
    given checkpoint.
    """
    # Add non-threadsafe checkpoint generation to throw in a bit of time so
    # that calls are more likely to disinterleave.
    new_checkpoint = self._generate_checkpoint()

    with self._record_lock:
      record, new_checkpoint = self._checkpoint_history(new_checkpoint)
      stamp = self._checkpoint[new_checkpoint][0]  # Most recently-seen index
      index = stamp - self._history_epoch
      # Map and filter to retrieve non-None records
      records = tuple(
        filter(lambda r: r is not None,
        map(lambda h: h[0],
          self._history[index + 1:])))
      self._checkpoint[new_checkpoint][1] += 1

      self._checkpoint[checkpoint][1] -= 1
      # If refcount is 0, delete the checkpoint and attempt to reap history
      if not self._checkpoint[checkpoint][1]:
        del self._checkpoint[checkpoint]
        earliest = min(self._checkpoint.values())
        if earliest - self._history_epoch:
          self._history = self._history[earliest - self._history_epoch:]
          self._history_epoch = earliest
      return checkpoint, records
