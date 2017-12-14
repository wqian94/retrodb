""" Library for base classes of retroactively-updatable views. """

import numbers


class NumericView(numbers.Integral):
  """ Wrapper for numeric functions so that views can behave like numbers. """
  # General magic functions
  def __repr__(self):
    return self._value.__repr__()

  # Number-related functions
  def __abs__(self, *args, **kwargs):
    return self._value.__abs__(*args, **kwargs)

  def __add__(self, *args, **kwargs):
    return self._value.__add__(*args, **kwargs)

  def __and__(self, *args, **kwargs):
    return self._value.__and__(*args, **kwargs)

  def __ceil__(self, *args, **kwargs):
    return self._value.__ceil__(*args, **kwargs)

  def __eq__(self, *args, **kwargs):
    return self._value.__eq__(*args, **kwargs)

  def __floor__(self, *args, **kwargs):
    return self._value.__floor__(*args, **kwargs)

  def __floordiv__(self, *args, **kwargs):
    return self._value.__floordiv__(*args, **kwargs)

  def __int__(self, *args, **kwargs):
    return self._value.__int__(*args, **kwargs)

  def __invert__(self, *args, **kwargs):
    return self._value.__invert__(*args, **kwargs)

  def __le__(self, *args, **kwargs):
    return self._value.__le__(*args, **kwargs)

  def __lshift__(self, *args, **kwargs):
    return self._value.__lshift__(*args, **kwargs)

  def __lt__(self, *args, **kwargs):
    return self._value.__lt__(*args, **kwargs)

  def __mod__(self, *args, **kwargs):
    return self._value.__mod__(*args, **kwargs)

  def __mul__(self, *args, **kwargs):
    return self._value.__mul__(*args, **kwargs)

  def __neg__(self, *args, **kwargs):
    return self._value.__neg__(*args, **kwargs)

  def __or__(self, *args, **kwargs):
    return self._value.__or__(*args, **kwargs)

  def __pos__(self, *args, **kwargs):
    return self._value.__pos__(*args, **kwargs)

  def __pow__(self, *args, **kwargs):
    return self._value.__pow__(*args, **kwargs)

  def __radd__(self, *args, **kwargs):
    return self._value.__radd__(*args, **kwargs)

  def __rand__(self, *args, **kwargs):
    return self._value.__rand__(*args, **kwargs)

  def __rfloordiv__(self, *args, **kwargs):
    return self._value.__rfloordiv__(*args, **kwargs)

  def __rlshift__(self, *args, **kwargs):
    return self._value.__rlshift__(*args, **kwargs)

  def __rmod__(self, *args, **kwargs):
    return self._value.__rmod__(*args, **kwargs)

  def __rmul__(self, *args, **kwargs):
    return self._value.__rmul__(*args, **kwargs)

  def __ror__(self, *args, **kwargs):
    return self._value.__ror__(*args, **kwargs)

  def __round__(self, *args, **kwargs):
    return self._value.__round__(*args, **kwargs)

  def __rpow__(self, *args, **kwargs):
    return self._value.__rpow__(*args, **kwargs)

  def __rrshift__(self, *args, **kwargs):
    return self._value.__rrshift__(*args, **kwargs)

  def __rshift__(self, *args, **kwargs):
    return self._value.__rshift__(*args, **kwargs)

  def __rtruediv__(self, *args, **kwargs):
    return self._value.__rtruediv__(*args, **kwargs)

  def __rxor__(self, *args, **kwargs):
    return self._value.__rxor__(*args, **kwargs)

  def __truediv__(self, *args, **kwargs):
    return self._value.__truediv__(*args, **kwargs)

  def __trunc__(self, *args, **kwargs):
    return self._value.__trunc__(*args, **kwargs)

  def __xor__(self, *args, **kwargs):
    return self._value.__xor__(*args, **kwargs)
