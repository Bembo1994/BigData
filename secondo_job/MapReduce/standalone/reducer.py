#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime
from collections import defaultdict
import collections

ticker_data_close_price = defaultdict(float)
ticker_data_volume = defaultdict(int)
dates = set()
sector_ticker = defaultdict(set)


def calculate_variation_percentage(xf,xi): 
  return ((xf-xi)/xi)*100


def get_all_dates(anno,d) : 
  app = set()
  for a in d:
    if datetime.fromtimestamp(int(a)).year == int(anno):
      app.add(a)
  return min(app),max(app)

for line in sys.stdin:

  bits = line.split("\t")

  if len(bits) == 4 :
    #if filter_anno(str(bits[1].rsplit("-")[0])):
    ticker_data_close_price[str(bits[0])+"_"+str(bits[1])] = float(bits[2])
    ticker_data_volume[str(bits[0])+"_"+ str(str(bits[1]).rsplit("-")[0])] += int(bits[3])
    dates.add(datetime.strptime(str(bits[1]), "%Y-%m-%d").timestamp())
  elif len(bits) == 2 :
    sector_ticker[bits[1]].add(bits[0])

sector_ticker = collections.OrderedDict(sorted(sector_ticker.items()))

for sector in sector_ticker :
  for i in range(2009,2019) : 
    xi_sector = 0
    xf_sector = 0
    max_ticker_variation = float('-inf')
    ticker_variation = None
    ticker_volume = None
    variation = 0
    max_vol = 0
    data_min,data_max = get_all_dates(i,dates)
    for ticker in sector_ticker.get(sector) :

      flag_1 = False
      flag_2 = False

      k = str(ticker)+"_"+str(datetime.fromtimestamp(int(data_min))).rsplit(" ")[0] #key_xi
      if str(k) in ticker_data_close_price and not flag_1 :
        xi_sector += float(ticker_data_close_price.get(str(k)))
        xi_ticker = float(ticker_data_close_price.get(str(k)))
        flag_1 = True

      k = str(ticker)+"_"+str(datetime.fromtimestamp(int(data_max))).rsplit(" ")[0]#key_xf
      if str(k) in ticker_data_close_price and not flag_2 :
        xf_sector += float(ticker_data_close_price.get(str(k)))
        xf_ticker = float(ticker_data_close_price.get(str(k)))
        flag_2 = True

      if flag_1 and flag_2 :
        variation_ticker_app = calculate_variation_percentage(xf_ticker,xi_ticker)
        if variation_ticker_app > max_ticker_variation:
          max_ticker_variation = variation_ticker_app
          ticker_variation = str(ticker)
      
      if str(ticker)+"_"+str(i) in ticker_data_volume:
        tmp  = ticker_data_volume.get(str(ticker)+"_"+str(i))
        if tmp > max_vol:
          max_vol = tmp
          ticker_volume = ticker

    if xi_sector != 0 :
      variation = calculate_variation_percentage(xf_sector,xi_sector)
    v = '%.3f'%float(variation)
    mv = '%.3f'%float(max_ticker_variation)
    print("{}\t{}\t{}%\t{}\t{}%\t{}\t{}".format(str(sector).replace("\n",""), i, v, ticker_variation, mv, ticker_volume, max_vol))



        
  

  


