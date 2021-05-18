#!/usr/bin/env python3
"""reducer.py"""

import sys
import collections 

d = {}

for line in sys.stdin:

  bits = line.split("\t")
  #sector, i, variation, ticker_variation, max_ticker_variation, ticker_volume, avg
  d[str(str(bits[0])+"\t"+str(bits[1]))] = [bits[2],bits[3],bits[4],bits[5],bits[6]]

d = collections.OrderedDict(sorted(d.items()))

print("SETTORE\tANNO\tVARIAZIONE PERCENTUALE\tAZIONE\tVARIAZIONE PERCENTUALE\tAZIONE\tVOLUME")
for sect_year,item in d.items():
  var_year_truncate = '%.3f'%float(item[0])
  var_ticker_truncate = '%.3f'%float(item[2])
  print("{}\t{}%\t{}\t{}%\t{}\t{}".format(sect_year, var_year_truncate, item[1], var_ticker_truncate, item[3], item[4]))


