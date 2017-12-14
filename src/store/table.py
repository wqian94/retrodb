# table.py
#
# Representation of a database table, which contains the table schema and the
# data.

import time


# class Table
#
# Contains an immutable schema and mutable columns.
#
# Currently implemented as a key-value store.
class Table(dict):
  def __init__(self):
    self._keys = {}  # Time mapped to keys
    self._store = {}  # The kv store

  def __delitem__(self, key):
    return self.delete(self._keys.find(key))

  def __getitem__(self, key):
    return self._store[key]

  def __setitem__(self, key, value):
    return self.insert(key, value)

  # insert
  #
  # Inserts the given key-value pair into the table at the given time. Time
  # defaults to current time.
  #
  # Args:
  #   key (hashable): the key to use
  #   value (object): the value to associate
  #   t (int): the timestamp to insert at. Defaults to None, which indicates
  #      to use current time.
  #
  # Returns the timestamp of this transaction.
  def insert(self, key, value, t=None):
    if t is None:
      t = time.time()
    self._keys[t] = key
    self._store[key] = value
    return t

  # delete
  #
  # Deletes the transaction at time t. Returns the (key, value) pair that was
  # deleted, or None if no such transaction exists.
  #
  # Args:
  #   t (int): the timestamp of the transaction to delete
  #
  # Returns (key, value) or None.
  def delete(self, t):
    if t not in self._keys:
      return None

    key = self._keys[t]
    value = self._store[key]
    del self._keys[t], self._store[key]
    return (key, value)
