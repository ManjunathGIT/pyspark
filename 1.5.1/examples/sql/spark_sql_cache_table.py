# coding=utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import Row
import random

conf = SparkConf().setAppName("spark_sql_cache_table")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

dataRDD = sc.textFile("hdfs://dip.cdh5.dev:8020/user/yurun/datas").map(lambda line: line.split(
    "\t")).map(lambda words: Row(col1=words[0], col2=words[1], col3=words[2]))

sourceRDD = hc.inferSchema(dataRDD)

sourceRDD.registerAsTable("source")

rows = hc.sql("select * from source limit 10").collect()

for row in rows:
    print row

sc.stop()
