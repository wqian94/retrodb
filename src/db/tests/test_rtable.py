""" Tests for RTable. """

import db.rtable as rtable
import db.types.numbers as rnum
import db.types.strings as rstr

def test_simple_flow():
  """ Tests simple insertions and deletions. """
  table = rtable.RTable(key=rnum.Int, value=rstr.String)

  t1, k1, v1 = 1, 42, 'basil'
  r1 = table.insert(t1, key=k1, value=v1)

  t2, k2, v2 = 3, 21, 'eggplant'
  r2 = table.insert(t2, key=k2, value=v2)

  print(table)
  print(repr(table))
  print(repr(table._records[t1][0]))

  table.delete(t1 + 1, r1)

  print(table)

  table.erase(t2)

  print(table)

  raise Exception()
