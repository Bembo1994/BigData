#!/usr/bin/env python3
"""mapper.py"""

import sys
import datetime

TICKER = 0
OPEN = 1
CLOSE = 2
ADJ_CLOSE = 3
LOW = 4
HIGH = 5
VOLUME = 6
DATE = 7

for line in sys.stdin:

  bits = line.strip().split(",")
  if str(bits[TICKER]).islower():
    continue
  ticker = bits[TICKER]
  data = int(datetime.datetime.strptime(str(bits[DATE]), "%Y-%m-%d").timestamp())
  open_ = bits[OPEN]
  close = bits[CLOSE]
  minimum = bits[LOW]
  maximum = bits[HIGH]

  print("{}\t{}\t{}\t{}\t{}\t{}".format(str(ticker), str(data), str(open_), str(close), str(minimum), str(maximum)))


      
