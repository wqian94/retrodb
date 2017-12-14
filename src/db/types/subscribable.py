""" Library for subscribably types to manage subscription. """

import abc
import collections
import random
import threading

import db.rrecord as rrecord


class Subscribable(abc.ABC):
  """ Abstract superclass for subscribable types, such as tables and views. """
  def __init__(self):
    # Use a two-way mapping for checkpoint <=> history stamp associations
    self._checkpoint_map = {}  # Checkpoint -> (history stamp, refcount)
    self._history_map = {}  # (History index = stamp - epoch) -> checkpoint

    self._history = []  # [History index] = record
    self._history_epoch = 0  # The logical history stamp of history_map[0]

    self._pubsub_lock = threading.RLock()
    self._pubsub_cond = threading.Condition(self._pubsub_lock)

  def __del__(self):
    """ Calls free() to release all subscribers. """
    self.free()

  def _add_to_history(self, *records):
    """
    Add the specified record(s) to history.

    Is thread-safe.

    Args:
      records (tuple(rrecord.Record)): the records to add to history
    """
    with self._pubsub_lock:
      self._history += list(records)
      self._pubsub_cond.notify_all()

  @abc.abstractmethod
  def _all_records(self):
    """ Returns all records thus far in history as a tuple. """
    pass

  def _checkpoint_now(self, checkpoint):
    """
    Returns a checkpoint to indicate that history, up to the most recent item,
    has been processed.

    If the checkpoint for the most recent historical record does not already
    exist, a checkpoint for it will be generated, using the checkpoint
    parameter as the preferred checkpoint marker.

    Is thread-safe.

    Args:
      checkpoint (int): the preferred checkpoint to use

    Returns the checkpoint to the latest marker.
    """
    with self._pubsub_lock:
      stamp = len(self._history) + self._history_epoch
      if stamp not in self._history_map:
        # Ensure that we have a valid checkpoint marker
        checkpoint = self._generate_checkpoint(checkpoint)
        self._history_map[stamp] = checkpoint
        self._checkpoint_map[checkpoint] = (stamp, 1)
      else:
        checkpoint = self._history_map[stamp]
        stamp, refcount = self._checkpoint_map[checkpoint]
        self._checkpoint_map[checkpoint] = (stamp, refcount + 1)
      return self._history_map[stamp]

  def _generate_checkpoint(self, checkpoint=None):
    """
    Returns a checkpoint not currently in use. If the checkpoint parameter is
    None or already in use, a new, unused checkpoint marker will be returned
    instead.

    Not thread-safe.

    Args:
      checkpoint (int): the preferred checkpoint to use. Defaults to None.
    """
    while (checkpoint is None) or (checkpoint in self._checkpoint_map):
      checkpoint = random.randint(0, (1 << 64) - 1)
    return checkpoint

  def _remove_from_history(self, *records):
    """
    Removes the given records from history. Note that the records are
    internally replaced with None values, and the epoch does not change. This
    only prevents future calls to subscribe() from receiving those records.

    Note that this also preserves checkpoints easily.

    Is thread-safe.

    Args:
      records (tuple(rrecord.Record)): the records to remove from history
    """
    with self._pubsub_lock:
      for index in range(len(self._history)):
        if self._history[index] in records:
          self._history[index] = None
      if len(records):
        self._history += (rrecord.Erasure(records[0].time, *records),)
      self._pubsub_cond.notify_all()

  def free(self):
    """
    Manually ensures that all subscribers are unsubscribed before deallocating
    resources.
    """
    with self._pubsub_cond:
      self._pubsub_cond.notify_all()
    cond = threading.Condition()
    with cond:
      while 0 < len(self._checkpoint_map):
        cond.wait(0.1)

  def subscribe(self, checkpoint=None, timeout=None):
    """
    Returns the changes since the provided checkpoint, along with a new
    checkpoint marker for the latest observed historical record.

    If checkpoint is None, all history will be returned. Otherwise, only
    changes since checkpoint will be returned.

    If a checkpoint association already exists for the latest observed
    historical record, then this will use the existing association. Otherwise,
    this will create a new association.

    Args:
      checkpoint (int): the checkpoint marker of the latest-seen historical
        record. If checkpoint is not a valid checkpoint, it is treated as None.
        Defaults to None (a.k.a. the beginning of time)
      timeout (int): the timeout factor for this subscription. if 

    Returns a (new checkpoint, tuple of change records) tuple.
    """
    # Add some unsynchronized precomputing, which saves time in the lock and
    # potentially can de-interleave concurrent calls.
    new_checkpoint = self._generate_checkpoint()

    with self._pubsub_cond:
      # Invalid values of the checkpoint are treated as None
      if checkpoint not in self._checkpoint_map:
        checkpoint = None

      # If a valid checkpoint, wait for when there is a new history item
      if (checkpoint is not None) and (not self._pubsub_cond.wait_for(
          lambda: len(self._history) + self._history_epoch >
            self._checkpoint_map[checkpoint][0],
          timeout=timeout)):
        return checkpoint, ()

      new_checkpoint = self._checkpoint_now(new_checkpoint)

      if checkpoint:
        old_index = self._checkpoint_map[checkpoint][0] - self._history_epoch
        new_index = \
          self._checkpoint_map[new_checkpoint][0] - self._history_epoch
        records = self._history[old_index:new_index]

        # Decrease the refcount and do cleanup
        self.unsubscribe(checkpoint)
      else:
        records = self._all_records()

      # Remove None records
      records = tuple(filter(lambda record: record, records))

      return (new_checkpoint, records)

  def unsubscribe(self, checkpoint):
    """
    Unsubscribes a subscriber of the given checkpoint.

    Args:
      checkpoint (int): the checkpoint marker from which to unsubscribe.
    """
    with self._pubsub_lock:
      stamp, refcount = self._checkpoint_map[checkpoint]
      refcount -= 1
      self._checkpoint_map[checkpoint] = (stamp, refcount)

      # Refcount depleted, time to remove the checkpoint
      if 0 == refcount:
        del self._checkpoint_map[checkpoint], self._history_map[stamp]

        # Potentially shift the history map
        keys = self._history_map.keys()
        if len(keys):
          oldest_stamp = min(keys)
          if oldest_stamp > stamp:
            diff = oldest_stamp - stamp
            self._history = self._history[diff:]
            self._history_epoch += diff
