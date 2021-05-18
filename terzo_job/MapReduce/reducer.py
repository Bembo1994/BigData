#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime
from collections import defaultdict
import collections

SOGLIA = 1
data_ticker_close_price = defaultdict(float)
tickers = set()
ticker_variation = defaultdict(list)
date_dict = defaultdict(set)

def calculate_variation_percentage(xf,xi) : 
  return ((xf-xi)/xi)*100


for line in sys.stdin :
  bits = line.split("\t")
  data_ticker_close_price[str(bits[1])+"_"+str(bits[0])] = float(bits[2])
  tickers.add(str(bits[0]))
  mese = datetime.fromtimestamp(int(bits[1])).month
  date_dict[mese].add(bits[1])

for month in range(1,13) :

  data_key_xi = min(date_dict.get(month))
  data_key_xf = max(date_dict.get(month))

  for ticker in tickers:
    xi = 0
    xf = 0
    di = str(data_key_xi)+"_"+str(ticker)
    df = str(data_key_xf)+"_"+str(ticker)
    if di in data_ticker_close_price.keys() and df in data_ticker_close_price.keys() :
      xi = data_ticker_close_price.get(di)
      xf = data_ticker_close_price.get(df)
      variation_percentage = calculate_variation_percentage(xf,xi)
    else :
      variation_percentage = 0    
    v_trunc = '%.3f'%(variation_percentage)
    ticker_variation[ticker].append(v_trunc)


for item in ticker_variation.items():
  print(str(item).replace("(","").replace(")","").replace("'","").replace(" ","").replace("[","").replace("]",""))



