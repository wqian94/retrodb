""" Library for the retroactively-updatable views. """

import threading

import baseviews


class Sum(baseviews.NumericView):
  """
  View for retroactively-updatable Sum.

  Since Sum is invertible and commutative, its implementation is fairly simple.
  """
  def __init__(self, store, index):
    self._index = index
    self._store = store
    self._value = sum([
      record.values()[index] for record in store._records.values()])

    store.add_delete_callback(self._delete_callback)
    store.add_insert_callback(self._insert_callback)

  def _delete_callback(self, record):
    self._value -= record.values()[self._index]

  def _insert_callback(self, record):
    self._value += record.values()[self._index]


class Min(baseviews.NumericView):
  """
  View for retroactively-updatable Min.

  Uses several trees to bookkeep.
  """
  def __init__(self, store, index):
    self._index = index
    self._store = store
    self._value = min([
      record.values()[index] for record in store._records.values()])

    store.add_delete_callback(self._delete_callback)
    store.add_insert_callback(self._insert_callback)

  def _delete_callback(self, record):
    # Min might have changed
    if self._value == record.values()[self._index]:
      # TODO: improve this with a real retroactive data structure
      self._value = min([
        record.values()[self._index] \
        for record in self._store._records.values()])

  def _insert_callback(self, record):
    self._value = min(self._value, record.values()[self._index])
