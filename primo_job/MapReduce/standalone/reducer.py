#!/usr/bin/env python3
"""reducer.py"""

import sys
import datetime
from collections import defaultdict

ticker_data = defaultdict(list)
ticker_open = defaultdict(list)
ticker_close = defaultdict(list)
ticker_max = defaultdict(list)
ticker_min = defaultdict(list)

d = {}
#(Xf / Xi) x 100
def calculate_variation_percentage(xf,xi): 
  return ((xf-xi)/xi)*100

for line in sys.stdin:

  ticker, data, open_, close, low, high = line.split("\t")

  ticker_data[ticker].append(data)
  ticker_open[ticker].append(open_)
  ticker_close[ticker].append(close)
  ticker_min[ticker].append(low)
  ticker_max[ticker].append(high)


for elem in ticker_data:

  values_data = ticker_data.get(elem)
  values_data = list(map(int, values_data))

  data_min = min(values_data)
  pos_min = int(values_data.index(data_min))
  prima_data = str(datetime.datetime.fromtimestamp(int(data_min))).rsplit(" ")[0]

  data_max = max(values_data)
  pos_max = int(values_data.index(data_max))
  ultima_data = str(datetime.datetime.fromtimestamp(int(data_max))).rsplit(" ")[0]

  close_min = float(ticker_close.get(elem)[pos_min])
  close_max = float(ticker_close.get(elem)[pos_max])

  low_price = str(min(ticker_min.get(elem)))
  high_price = str(max(ticker_max.get(elem)))

  variation = str(calculate_variation_percentage(close_max, close_min))

  d[str(elem)] = [int(data_min), int(data_max), float(variation), float(low_price), float(high_price)]
  
d = dict(sorted(d.items(), key=lambda item: item[1][1], reverse=True))

print("TICKER\tDATA PRIMA QUOTAZIONE\tDATA ULTIMA QUOTAZIONE\tVARIAZIONE PERCENTUALE\tPREZZO MINIMO\tPREZZO MASSIMO")
for tick,item in d.items():
  a =  str(datetime.datetime.fromtimestamp(int(item[0]))).rsplit(" ")[0] 
  b = str(datetime.datetime.fromtimestamp(int(item[1]))).rsplit(" ")[0]
  c = '%.3f'%float(item[2])
  d1 = '%.3f'%float(item[3])
  d2 = '%.3f'%float(item[4])
  print("{}\t{}\t{}\t{}%\t{}\t{}".format(tick, a, b, c, d1, d2))
  
  
