#!/usr/bin/env python3
"""spark application"""
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from datetime import datetime

def get_max(x, y):
    if x[1] > y[1]:
        return x
    else:
        return y

def get_min(x, y):
    if x[1] > y[1]:
        return y
    else:
        return x

SOGLIA = 1

input_filepath_hsp = "file:///home/hadoop/historical_stock_prices.csv"
output_filepath = "file:///home/hadoop/output/"

spark = SparkSession.builder.appName("Secondo Job").getOrCreate()

input_RDD_hsp = spark.sparkContext.textFile(input_filepath_hsp).map(lambda line: line.split(",")).cache()
input_RDD_hsp = input_RDD_hsp.filter(lambda line: line[0] != "ticker").filter(lambda line: int(line[7].rsplit("-")[0]) == 2017)

#(ticker,close,data)
main_table = input_RDD_hsp.map(lambda line: (line[0],line[2], int(datetime.strptime(str(line[7]), "%Y-%m-%d").timestamp())))

main_table.persist(StorageLevel.MEMORY_AND_DISK)

# ((ticker, month), close)
ticker_month_close_data_max = main_table.map(lambda line: ((line[0], datetime.fromtimestamp(int(line[2])).month), (line[1],line[2]))).reduceByKey(lambda x, y: get_max(x, y))
ticker_month_close_data_min = main_table.map(lambda line: ((line[0], datetime.fromtimestamp(int(line[2])).month), (line[1],line[2]))).reduceByKey(lambda x, y: get_min(x, y))

ticker_month_var = ticker_month_close_data_max.join(ticker_month_close_data_min).map(lambda line : ((line[0][0],line[0][1]),float('%.3f'%(((float(line[1][0][0])-float(line[1][1][0]))/float(line[1][1][0]))*100)) )).sortBy(lambda line : (line[0][0],line[0][1])).map(lambda line : ((line[0][0]),(line[1]))).groupByKey().mapValues(lambda val: [e for e in val]).filter(lambda line : len(line[1])==12).map(lambda line : ((line[0]),line[1][0],line[1][1],line[1][2],line[1][3],line[1][4],line[1][5],line[1][6],line[1][7],line[1][8],line[1][9],line[1][10],line[1][11]  ))#.sortBy(lambda line : (line[0]))


prod_cart = ticker_month_var.cartesian(ticker_month_var).filter(lambda line : line[0] != line[1])

app = prod_cart.map(lambda line : ((line[0][0],line[1][0]),(abs(line[0][1]-line[1][1]), abs(line[0][2]-line[1][2]), abs(line[0][3]-line[1][3]), abs(line[0][4]-line[1][4]), abs(line[0][5]-line[1][5]), abs(line[0][6]-line[1][6]), abs(line[0][7]-line[1][7]), abs(line[0][8]-line[1][8]), abs(line[0][9]-line[1][9]), abs(line[0][10]-line[1][10]), abs(line[0][11]-line[1][11]), abs(line[0][12]-line[1][12]) )))
app2 = prod_cart.map(lambda line : ((line[0][0],line[1][0]),(line[0][1],line[1][1],line[0][2],line[1][2], line[0][3],line[1][3], line[0][4],line[1][4], line[0][5],line[1][5], line[0][6],line[1][6], line[0][7],line[1][7], line[0][8],line[1][8], line[0][9],line[1][9], line[0][10],line[1][10], line[0][11],line[1][11], line[0][12],line[1][12]) ))

result = app.filter(lambda line : line[1][0] <= SOGLIA and line[1][1] <= SOGLIA and line[1][2] <= SOGLIA and line[1][3] <= SOGLIA and line[1][4] <= SOGLIA and line[1][5] <= SOGLIA and line[1][6] <= SOGLIA and line[1][7] <= SOGLIA and line[1][8] <= SOGLIA and line[1][9] <= SOGLIA and line[1][10] <= SOGLIA and line[1][11] <= SOGLIA )

out = app2.join(result).map(lambda line : (line[0],line[1][0]))

out.coalesce(1, shuffle = True).saveAsTextFile(output_filepath)
