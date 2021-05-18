#!/usr/bin/env python3
"""reducer2.py"""

import sys
from collections import defaultdict

ticker_variation = defaultdict(list)
SOGLIA = 1


def check_if_similar(list_1, list_2) :
  flag = False
  for i in range(len(list_1)):  
    if abs(float(list_1[i]) - float(list_2[i])) <= SOGLIA and float(list_1[i]) != 0.0 and float(list_2[i]) != 0.0:
      flag = True
    else :
      return False
  return flag

for line in sys.stdin :

  bits = line.strip().split(",")
  if len(bits) == 13: # 1 ticker + 12 variazione dei mesi
    for i in range(1,13) : 
      ticker_variation[bits[0]].append(bits[i])

ld = list(ticker_variation.items())
length = len(ld)
for i,(t1,v1) in enumerate(ld):
  j = i+1
  while j < length :
    t2,v2 = ld[j]
    if check_if_similar(v1,v2) :     
      print("{},{}\t{}".format(t1,t2,list(zip(v1,v2))))
    j += 1
