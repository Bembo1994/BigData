#!/usr/bin/env python3
"""mapper.py"""

import sys

TICKER = 0
OPEN = 1
CLOSE = 2
ADJ_CLOSE = 3
LOW = 4
HIGH = 5
VOLUME = 6
DATE = 7

EXCHANGE = 1
NAME = 2
SECTOR = 3
INDUSTRY = 4


def filter_anno(anno):
  a = int(anno)
  if a >= 2009 and a<=2018:
    return True
  else :
    return False

for line in sys.stdin:
  #print(line)
  bits = line.strip().split(",")

  if str(bits[TICKER]).islower() or bits[TICKER+1].islower():
    continue


  if len(bits) == 8 and filter_anno(bits[DATE].rsplit("-")[0]) :
    ticker = bits[TICKER]
    data = bits[DATE]#.rsplit("-")[0] #anno
    close = bits[CLOSE]
    volume = bits[VOLUME]
    print("{}\t{}\t{}\t{}".format(str(ticker), str(data), str(close), str(volume)))

  #Pulendo i dati si è aggiunto l'indice ecco perchè i +1, quindi la lunghezza di bits diventa 6
  elif len(bits) == 6 :
    if bits[SECTOR+1] != None and bits[SECTOR+1] != "None" and bits[SECTOR+1]!="N/A" and bits[SECTOR+1]!="nan":
      ticker = bits[TICKER+1]
      sector = bits[SECTOR+1]
      print("{}\t{}".format(str(ticker), str(sector)))


      
