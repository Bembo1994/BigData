#!/usr/bin/env python3
"""spark application"""
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from datetime import datetime
from operator import add 


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


input_filepath_hsp = "file:///home/hadoop/historical_stock_prices.csv"
input_filepath_hs = "file:///home/hadoop/clean_hs.csv"
output_filepath = "file:///home/hadoop/output/"

spark = SparkSession.builder.appName("Secondo Job").getOrCreate()

input_RDD_hsp = spark.sparkContext.textFile(input_filepath_hsp).map(lambda line: line.split(",")).cache()
input_RDD_hsp = input_RDD_hsp.filter(lambda line: line[0] != "ticker").filter(lambda line: int(line[7].rsplit("-")[0]) >= 2009 and int(line[7].rsplit("-")[0]) <= 2018)

input_RDD_hs = spark.sparkContext.textFile(input_filepath_hs).map(lambda line: line.split(",")).cache()
input_RDD_hs = input_RDD_hs.filter(lambda line: line[1] != "ticker" and line[4] != "nan")


main_table = input_RDD_hsp.map(lambda line: (line[0], (line[2], line[6], datetime.strptime(line[7], "%Y-%m-%d").timestamp()))).join(input_RDD_hs.map(lambda line: (line[1], (line[4]))))

# (ticker, close, volume, date, settore)
main_table = main_table.map(lambda line: (line[0], float(line[1][0][0]), float(line[1][0][1]), line[1][0][2], line[1][1]))
main_table.persist(StorageLevel.MEMORY_AND_DISK)

# ((sector, year), sum_volume)
max_volume = main_table.map(lambda line: ((line[4], datetime.fromtimestamp(int(line[3])).year,line[0]), (line[2]))).reduceByKey(add).map(lambda line : ((line[0][0],line[0][1]),(line[0][2],line[1]))).reduceByKey(lambda x, y: get_max(x, y))


# ((sector, year), sum close of date max )
sector_close_year_data_max = main_table.map(lambda line: ((line[4],line[3]), line[1])).reduceByKey(add).map(lambda line : ((line[0][0],datetime.fromtimestamp(int(line[0][1])).year),(line[1],line[0][1]))).reduceByKey(lambda x, y: get_max(x, y)).map(lambda line : ((line[0][0],line[0][1]),line[1][0]))
# ((sector, year), sum close of date min)
sector_close_year_data_min = main_table.map(lambda line: ((line[4],line[3]), line[1])).reduceByKey(add).map(lambda line : ((line[0][0],datetime.fromtimestamp(int(line[0][1])).year),(line[1],line[0][1]))).reduceByKey(lambda x, y: get_min(x, y)).map(lambda line : ((line[0][0],line[0][1]),line[1][0]))

sector_variation_year = sector_close_year_data_max.join(sector_close_year_data_min).map(lambda line : ((line[0][0],line[0][1]),'%.3f'%(((line[1][0]-line[1][1])/line[1][1])*100)))


#((sector,year), ticker,var)
sector_ticker_var_data_max = main_table.map(lambda line: ((line[4],datetime.fromtimestamp(int(line[3])).year,line[0]),(line[1],line[3]))).reduceByKey(lambda x, y: get_max(x, y))
sector_ticker_var_data_min = main_table.map(lambda line: ((line[4],datetime.fromtimestamp(int(line[3])).year,line[0]),(line[1],line[3]))).reduceByKey(lambda x, y: get_min(x, y))
sector_ticker_var = sector_ticker_var_data_max.join(sector_ticker_var_data_min).map(lambda line : ((line[0][0],line[0][1]), (line[0][2], float('%.3f'%(((line[1][0][0]-line[1][1][0])/line[1][1][0])*100))))).reduceByKey(lambda x, y: get_max(x, y))


result = sector_variation_year.join(sector_ticker_var).join(max_volume).map(lambda line : (line[0][0],line[0][1],line[1][0][0],line[1][0][1][0],line[1][0][1][1],line[1][1][0],line[1][1][1] )).sortBy(lambda line : (line[0],line[1]))


result.coalesce(1,shuffle=True).saveAsTextFile(output_filepath)
