""" Library for a simple retroactively-updatable key-value store. """

import threading

import views


class RStore(object):
  """ A retroactively-updatable interface. """
  def __init__(self, cls):
    """
    Initializes a new retroactively-updatable key-value store, wrapping around
    the given storage class.

    The underlying class must support the mutators delete() and insert(), as
    well as the observer select().

    Note that this initializer creates a new instance of cls to wrap.

    Args:
      cls (class): the base storage class
    """
    self._store = cls()

  def add(self, t, op, *args):
    """
    Adds the operation op(args) to the underlying storage class at time t.

    Assumes no other operation already exists at time t.

    Args:
      t (int): the time to add the operation
      op (str): the operation to apply
      args (tuple(object)): the arguments to op
    """
    record = getattr(self._store, op)(*args)

    if "insert" == op:
      # Make callbacks
      for cb in self._store._insert_callbacks:
        threading.Thread(target=cb, args=(record,)).start()

  def erase(self, t):
    """
    Retroactively erases the operation added at time t.

    Assumes that there is an operation to erase at time t.

    Args:
      t (int): the time at which to erase an operation
    """
    record = self._store.delete(t)

    # Make callbacks
    for cb in self._store._delete_callbacks:
      threading.Thread(target=cb, args=(record,)).start()

  def observe(self, func, *args):
    """
    Observe the store at present time, with the given observation function and
    its arguments.

    Args:
      func (str): the observation function to invoke
      args (tuple(object)): the arguments to the observation function

    Returns the result of the observation.
    """
    return getattr(self._store, func)(*args)

  def __repr__(self):
    return "RStore -> " + str(self._store)


class Store(object):
  """ Contains the non-retroactive part of the key-value store. """
  def __init__(self):
    self._delete_callbacks = ()
    self._insert_callbacks = ()

    self._records = {}

  # Mutators
  def delete(self, key):
    """ Deletes the Record for the given key and returns the deleted record.

    Assumes key does exist in the Store.

    Args:
      key (hashable): the key whose Record we want to delete
    """
    record = self._records[key]
    del self._records[key]
    return record

  def insert(self, key, *values):
    """
    Creates and inserts a Record for the given key, and returns the created
    Record.

    Assumes key does not already exist in the Store.

    Args:
      key (hashable): the key to use
      values (tuple(object)): the value(s) to associate with the key
    """
    record = Record(key, *values)
    self._records[key] = record
    return record

  # Observers
  def min(self, index=0):
    """
    Returns the min of all the Records' values at the given index.

    Args:
      index (int): the index of the values over which we want to compute the
        min. Defaults to 0, the keys.
    """
    return views.Min(self, index)

  def select(self, key):
    """
    Returns the Record for the given key.

    Assumes key does exist in the Store.

    Args:
      key (hashable): the key whose Record we want to retrieve
    """
    return self._records[key]

  def sum(self, index=0):
    """
    Returns the sum of all the Records' values at the given index.

    Args:
      index (int): the index of the values we want to retrieve. Defaults to 0,
        the keys.
    """
    return views.Sum(self, index)

  # Callbacks
  def add_delete_callback(self, func):
    self._delete_callbacks += (func,)

  def add_insert_callback(self, func):
    self._insert_callbacks += (func,)

  # Magic functions
  def __repr__(self):
    records = (str(record) for record in self._records.values())
    return "Store (%d elements):\n  %s" % (
      len(self._records), "\n  ".join(records))


class Record(object):
  """
  Contains a single record of information, such as about the key, value(s),
  etc.
  """
  def __init__(self, key, *values):
    """
    Creates a Record for the provided key and value(s).

    Args:
      key (hashable): the key to use
      values (tuple(object)): the value(s) to associate with the key
    """
    self._key = key
    self._values = (key,) + values

  def key(self):
    """ Returns the Record's key. """
    return self._key

  def values(self):
    """ Returns the Record's associated value(s). """
    return self._values

  def __repr__(self):
    return "Record[%s |-> %s]" % (self._key, str(self._values))


def get_test():
  """ Testing store.py """
  s = RStore(Store)
  print("Empty RStore:", s)

  t, op, k, v = 3, "insert", 1, 2
  print("Adding %s(%s, %s) at time %d" % (op, k, v, t))
  s.add(t, op, k, v)
  print("State:", s)
  print("Sum(0):", s.observe("sum"))
  print("Sum(1):", s.observe("sum", 1))

  rsum = s.observe("sum", 1)
  print("Retrosum(1):", rsum)

  rmin = s.observe("min", 1)
  print("Retromin(1):", rmin)

  t, op, k, v = 1, "insert", 4, 5
  print("Adding %s(%s, %s) at time %d" % (op, k, v, t))
  s.add(t, op, k, v)
  print("State:", s)
  print("Sum(0):", s.observe("sum"))
  print("Sum(1):", s.observe("sum", 1))

  print("Retrosum(1):", rsum)

  print("Retromin(1):", rmin)

  t, op, k, v = 8, "insert", -1, -2
  print("Adding %s(%s, %s) at time %d" % (op, k, v, t))
  s.add(t, op, k, v)
  print("State:", s)
  print("Sum(0):", s.observe("sum"))
  print("Sum(1):", s.observe("sum", 1))

  print("Retrosum(1):", rsum)

  print("Retromin(1):", rmin)

  t, op, k = 8, "delete", 1
  print("Adding %s(%s) at time %d" % (op, k, t))
  s.add(t, op, k)
  print("State:", s)
  print("Sum(0):", s.observe("sum"))
  print("Sum(1):", s.observe("sum", 1))

  print("Retrosum(1):", rsum)

  print("Retromin(1):", rmin)

  t = 4
  print("Erasing op at time %d" % (t))
  s.erase(t)
  print("State:", s)
  print("Sum(0):", s.observe("sum"))
  print("Sum(1):", s.observe("sum", 1))

  print("Retrosum(1):", rsum)

  print("Retromin(1):", rmin)

  return s

if __name__ == '__main__':
  get_test()
