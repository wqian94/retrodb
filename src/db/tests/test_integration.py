""" Simple integration tests. """

import time

import db.rtable as rtable
import db.types.numbers as rnum
import db.types.predicates as rpred
import db.types.strings as rstr
import db.views.sum as rviewsum
import db.views.select as rviewselect

def test_simple_integration_workflow():
  """ Tests simple workflows that integrate several components together. """
  table = rtable.RTable(key=rnum.Int, value=rstr.String, comment=rstr.String)

  t1, k1, v1, c1 = 1, 42, 'basil', 'commented'
  r1 = table.insert(t1, key=k1, value=v1, comment=c1)

  s_key = table.sum(4, 'key')
  s_s_key = rviewsum.Sum(s_key, 6, 'SUM(key)')
  sel = table.select(7, 'key')
  sel2 = rviewselect.Select(sel, 8, 'key')
  s_sel = rviewsum.Sum(sel2, 9, 'key')
  sel_pred = table.select(
    5, 'key', predicates=(rpred.Where(lambda record: record['key'] % 2),))

  time.sleep(0.05)
  print('s_key =', s_key)
  print('s_s_key =', s_s_key)
  print('sel =', sel)
  print('sel2 =', sel2)
  print('s_sel =', s_sel)
  print('sel_pred =', sel_pred)

  t2, k2, v2 = 3, 21, 'eggplant'
  r2 = table.insert(t2, key=k2, value=v2)
  time.sleep(0.05)
  print('s_key =', s_key)
  print('s_s_key =', s_s_key)
  print('sel =', sel)
  print('sel2 =', sel2)
  print('s_sel =', s_sel)
  print('sel_pred =', sel_pred)

  table.delete(t1 + 1, r1)
  time.sleep(0.05)
  print('s_key =', s_key)
  print('s_s_key =', s_s_key)
  print('sel =', sel)
  print('sel2 =', sel2)
  print('s_sel =', s_sel)
  print('sel_pred =', sel_pred)

  table.erase(t2)
  time.sleep(0.05)
  print('s_key =', s_key)
  print('s_s_key =', s_s_key)
  print('sel =', sel)
  print('sel2 =', sel2)
  print('s_sel =', s_sel)
  print('sel_pred =', sel_pred)

  s_key.free()
  s_s_key.free()
  sel.free()
  sel2.free()
  s_sel.free()
  sel_pred.free()

  raise Exception('Raising exception to see stdout in test.')
