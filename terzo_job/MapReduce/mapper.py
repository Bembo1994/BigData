#!/usr/bin/env python3
"""mapper.py"""

import sys
from datetime import datetime

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
  data = datetime.strptime(str(bits[DATE]),"%Y-%m-%d")
  if data.year == 2017:
    ticker = bits[TICKER]
    data = data.timestamp()
    close = bits[CLOSE]
    print("{}\t{}\t{}".format(str(ticker),str(int(data)),str(close)))
