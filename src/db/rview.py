""" Library for supporting general functionality in retroactive views. """

import abc
import inspect
import numbers
import threading

import db.errors as errors
import db.rrecord as rrecord
import db.rtable as rtable


def wrap(cls, clsname=None, no_rewrite=('__class__', '__init__')):
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
      Defaults to ('__class__', '__init__')

  Returns a class that is the wrapped version of cls.
  """
  attributes = {
    '_value': errors.RViewValueUninitializedException(
      'Attempted to use a view with an uninitialized value. ' +
      'Remember to initialize values in a view. ' +
      'See the documentation of rview.RView for details.'),
    '_wrapped_cls': cls,
  }
  for name, func in inspect.getmembers(cls, predicate=inspect.isfunction):
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
class RView(wrap(object, 'RView'), abc.ABC):
  def __init__(self, obj, *args, **kwargs):
    """
    Initializes a callback thread that waits on the given RView or RTable
    object for publish-subscribe updates. This thread is stored in the
    _cb_thread member variable.

    Args:
      obj (RView | RTable): a RView or RTable object on which to await for
        pub-sub changes
      args (list): positional argments to pass up the MRO chain
      kwargs (dict): keyword arguments to pass up the MRO chain

    Raises a RViewInitException if obj is not a RView instance.
    """
    super(self.__class__, self).__init__(*args, **kwargs)

    if (not isinstance(obj, RView)) and (not isinstance(obj, rtable.RTable)):
      raise errors.RViewInitException(
        'Parameter of type %s is not a RView or RTable object' % \
        obj.__class__.__name__)

    # Subscribe to current state and apply
    checkpoint, changes = obj.subscribe()
    self._apply_changes(changes)

    # Signals for quiet exits
    self._pubsub_exit_event = threading.Event()  # Signal to exit
    self._pubsub_exited_event = threading.Event()  # Signal of successful exit

    # Start callback thread
    self._cb_thread = threading.Thread(
      target=self._callback, args=(obj, checkpoint))
    self._cb_thread.start()

  def __del__(self):
    """ Does thread cleanup on the callback thread. """
    self._pubsub_exit_event.set()
    self._pubsub_exited_event.wait()

  def _apply_changes(self, changes):
    """
    Apply the changelist in changes to the current state.

    Args:
      changes (list(rview.RViewChanges)): the list of changes
    """
    for change in changes:
      if rrecord.Record.INSERT == change.type:
        self._callback_insert(record)
      elif rrecord.Record.DELETE == change.type:
        self._callback_delete(record)
      else:  # Change type is not part of enumeration
        # TODO: handle this case
        pass

  def _callback(self, obj, checkpoint):
    """
    Body of the callback thread. The sole parameter is the object to which this
    callback is subscribing.

    Args:
      obj (RView | RTable): the object that has already been verified to be of
        type RView or RTable
      checkpoint (int): the initial marker for when the last set of changes
        were seen by this subscriber
    """
    while not self._pubsub_exit_event.is_set():
      checkpoint, changes = obj.subscribe_since(checkpoint)
      self._apply_changes(changes)
    obj.unsubscribe()

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
  def _callback_insert(self, change):
    """
    Callback method for handling the case of rrecord.Record.INSERT for a single
    change.

    Args:
      record (rrecord.Record): the insertion record to make
    """
    pass

  def subscribe(self):
    """ Returns the current view as a checkpoint and changelist of records. """
    pass

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
    pass


# Some extended RView abstract subclasses
class RViewIntegrable(RView, wrap(numbers.Integral, 'RViewIntegrable')): pass
class RViewString(RView, wrap(str, 'RViewString')): pass
