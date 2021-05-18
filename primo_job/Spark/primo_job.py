#!/usr/bin/env python3
"""spark application"""

from pyspark.sql import SparkSession
from pyspark import StorageLevel
from datetime import datetime

def min_close(x, y):
    if x[1] > y[1]:
        return y
    else:
        return x

def max_close(x, y):
    if x[1] > y[1]:
        return x
    else:
        return y

input_filepath = "file:///home/hadoop/historical_stock_prices.csv"
output_filepath = "file:///home/hadoop/output/"

# initialize SparkSession
# with the proper configuration
spark = SparkSession.builder.appName("Primo Job").getOrCreate()

input_RDD = spark.sparkContext.textFile(input_filepath).map(lambda line: line.split(",")).cache()
input_RDD = input_RDD.filter(lambda line: line[0] != "ticker")

#(ticker,close,low,high,data)
main_table = input_RDD.map(lambda line: (line[0],line[2],line[4],line[5],datetime.strptime(line[7], "%Y-%m-%d").timestamp()))
main_table.persist(StorageLevel.MEMORY_AND_DISK)

ticker_min_date = main_table.map(lambda line : (line[0],line[4])).reduceByKey(min)
ticker_max_date = main_table.map(lambda line : (line[0],line[4])).reduceByKey(max)

# (ticker, (close, first_data))
first_data_close = main_table.map(lambda line: (line[0], (line[1],line[4]))).reduceByKey(lambda x, y: min_close(x, y)).map(lambda row : (row[0],row[1][0],row[1][1]))
# (ticker, (close, last_data))
last_data_close = main_table.map(lambda line: (line[0], (line[1],line[4]))).reduceByKey(lambda x, y: max_close(x, y)).map(lambda row : (row[0],row[1][0],row[1][1]))

ticker_price_min = main_table.map(lambda line: (line[0],line[2])).reduceByKey(min).map(lambda line : (line[0],float('%.3f'%(float(line[1])))))

ticker_price_max = main_table.map(lambda line: (line[0],line[3])).reduceByKey(max).map(lambda line : (line[0],float('%.3f'%(float(line[1])))))

ticker_close = first_data_close.join(last_data_close).map(lambda line : (line[0],'%.3f'%(((float(line[1][1])-float(line[1][0]))/float(line[1][0]))*100)))


result = ticker_min_date.join(ticker_max_date).join(ticker_close).map(lambda line : (line[0],(line[1][0][0],line[1][0][1],line[1][1]))).join(ticker_price_min).map(lambda line : (line[0],(line[1][0][0],line[1][0][1],line[1][0][2],line[1][1]))).join(ticker_price_max).map(lambda line : (line[0],(line[1][0][0],line[1][0][1],line[1][0][2],line[1][0][3],line[1][1])))

out  = result.sortBy(lambda line : line[1][1],ascending=False).map(lambda line : "{}\t{}\t{}\t{}\t{}\t{}".format(line[0],str(datetime.fromtimestamp(int(line[1][0]))).rsplit(" ")[0],str(datetime.fromtimestamp(int(line[1][1]))).rsplit(" ")[0] ,line[1][2],line[1][3],line[1][4]))

out.coalesce(1, shuffle = True).saveAsTextFile(output_filepath)


