""" Library for supporting general functionality in retroactive views. """

import abc
import inspect
import numbers
import threading

import db.errors as errors
import db.rrecord as rrecord
import db.types.subscribable as rpubsub


def wrap(cls, clsname=None, no_rewrite=()):
  """
  Wraps a class so that all its inherited functions operate dynamically on the
  _value member variable. By default, the wrapped class also inherits the name
  of cls, except with a suffix of '<retro>'.

  If _value is not properly initialized before use, its value will be a
  RViewValueUninitializedException. If cls defines _value, that will also
  validly overwrite _value.

  Args:
    cls (class): the class to wrap
    clsname (str): the desired name for the new class. Defaults to None, which
      indicates to use cls's name appended with '<retro>'
    no_rewrite (str): a list of functions that should not be (re)written.
      Defaults to an empty tuple

  Returns a class that is the wrapped version of cls.
  """
  attributes = {
    '_value': errors.RViewValueUninitializedException(
      'Attempted to use a view with an uninitialized value. ' +
      'Remember to initialize values in a view. ' +
      'See the documentation of rview.RView for details.'),
    '_wrapped_cls': cls,
  }
  modfuncs = ('__repr__', '__str__')
  modfuncs = [(name, getattr(cls, name)) for name in modfuncs]
  modfuncs += inspect.getmembers(cls, predicate=inspect.isfunction)
  for name, func in modfuncs:
    if (name not in no_rewrite) and (not getattr(func, '_rwrapped', False)):
      attributes[name] = lambda self, *args, **kwargs: \
        getattr(self._value, name)(*args, **kwargs)

      # Prevents over-wrapping of a wrapped function
      setattr(attributes[name], '_rwrapped', True)

  if clsname is None:
    clsname = cls.__name__ + '<retro>'

  return type(clsname, (cls,), attributes)


# Below is a set of wrapped classes that can be used to superclass retroactive
# views.
class RView(wrap(object), rpubsub.Subscribable, abc.ABC):
  PUBSUB_INITIAL_TIMEOUT = 0.1
  PUBSUB_MAX_TIMEOUT = 10

  def __init__(self, obj, *args, **kwargs):
    """
    Initializes a callback thread that waits on the given Subscribable object
    for publish-subscribe updates. This thread is stored in the
    _cb_thread member variable.

    Args:
      obj (subscribable): a Subscribable object on which to await for pub-sub
        changes
      args (tuple): positional argments to pass up the MRO chain
      kwargs (dict): keyword arguments to pass up the MRO chain

    Raises a RViewInitException if obj is not a Subscribable object.
    """
    super(RView, self).__init__(*args, **kwargs)

    if not isinstance(obj, rpubsub.Subscribable):
      raise errors.RViewInitException(
        'Parameter of type %s is not a Subscribable object' % \
        obj.__class__.__name__)

    # Signals for quiet exits
    self._pubsub_exit_event = threading.Event()  # Signal to exit
    self._pubsub_exited_event = threading.Event()  # Signal of successful exit

    # Start callback thread
    self._cb_thread = threading.Thread(
      target=self._callback, args=(obj, None))

  def __del__(self):
    """ Calls free() to perform thread cleanup on the callback thread. """
    super(RView, self).__del__()
    self.free()

  def _all_records(self):
    return self._value

  def _apply_changes(self, changes):
    """
    Apply the changelist in changes to the current state.

    Args:
      changes (list(rrecord.Record)): the list of changes
    """
    for record in changes:
      if rrecord.Record.INSERT == record.action:
        self._callback_insert(record)
      elif rrecord.Record.DELETE == record.action:
        self._callback_delete(record)
      elif rrecord.Record.ERASE == record.action:
        self._callback_erase(record.time, record.records)
      else:  # Change type is not part of enumeration
        # TODO: handle this case
        pass

  def _callback(self, obj, checkpoint):
    """
    Body of the callback thread. The sole parameter is the object to which this
    callback is subscribing.

    Args:
      obj (rpubsub.Subscribable): the object that has already been verified to
        be Subscribable
      checkpoint (int): the initial marker for when the last set of changes
        were seen by this subscriber
    """
    timeout = RView.PUBSUB_INITIAL_TIMEOUT
    while not self._pubsub_exit_event.is_set():
      new_checkpoint, changes = obj.subscribe(checkpoint, timeout=timeout)
      if new_checkpoint == checkpoint:  # No changes
        # Exponential backoff
        timeout = min(RView.PUBSUB_MAX_TIMEOUT, 2 * timeout)
      else:
        # Reset exponential backoff
        timeout = RView.PUBSUB_INITIAL_TIMEOUT
        checkpoint = new_checkpoint
        self._apply_changes(changes)
    self._pubsub_exited_event.set()
    obj.unsubscribe(checkpoint)

  @abc.abstractmethod
  def _callback_delete(self, record):
    """
    Callback method for handling the case of rrecord.Record.DELETE for a single
    change.

    Args:
      record (rrecord.Record): the deletion record to make
    """
    pass

  @abc.abstractmethod
  def _callback_erase(self, time, records):
    """
    Callback method for handling the case of rrecord.Record.ERASE for a single
    change, invoking retroactive erasure.

    Args:
      time (int): the time at which these records were added
      records (rrecord.Record): the records to retroactively erase
    """
    pass

  @abc.abstractmethod
  def _callback_insert(self, change):
    """
    Callback method for handling the case of rrecord.Record.INSERT for a single
    change.

    Args:
      record (rrecord.Record): the insertion record to make
    """
    pass

  def _start(self):
    """
    Starts the callback thread. Manually called by subclasses after
    initialization, so that callbacks can be correctly completed.
    """
    self._cb_thread.start()

  def free(self):
    """ Performs cleanup, such as on the callback thread. """
    self._pubsub_exit_event.set()
    self._pubsub_exited_event.wait()


# Some extended RView abstract subclasses
class RViewIntegrable(RView, wrap(numbers.Integral)): pass
class RViewString(RView, wrap(str)): pass
