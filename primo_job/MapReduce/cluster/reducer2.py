#!/usr/bin/env python3
"""reducer2.py"""

import sys

d = {}

for line in sys.stdin:

  tick, a, b, c, d1, d2 = line.split("\t")
  d[str(tick)] = [str(a), str(b), str(c), str(d1), str(d2)]
  
d = dict(sorted(d.items(), key=lambda item: item[1][1], reverse=True))

print("TICKER\tDATA PRIMA QUOTAZIONE\tDATA ULTIMA QUOTAZIONE\tVARIAZIONE PERCENTUALE\tPREZZO MINIMO\tPREZZO MASSIMO")
for tick,item in d.items():
  var_truncate = '%.3f'%float(item[2])
  d1 = '%.3f'%float(item[3])
  d2 = '%.3f'%float(item[4])
  print("{}\t{}\t{}\t{}%\t{}\t{}".format(tick, item[0], item[1], var_truncate, d1, d2))
  
