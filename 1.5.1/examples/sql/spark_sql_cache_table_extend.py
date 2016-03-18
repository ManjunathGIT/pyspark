# coding=utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import Row
import random

conf = SparkConf().setAppName("spark_sql_cache_table_extend")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

dataRDD = sc.textFile("/user/hdfs/rawlog/app_weibomobile03x4ts1kl_mwb_interface/").map(lambda line: line.split(
    ",")).map(lambda words: Row(col1=words[0], col2=words[1], col3=words[2]))

sourceRDD = hc.inferSchema(dataRDD)

sourceRDD.registerAsTable("source")

cacheRDD = hc.sql("select * from source")

# cacheRDD.registerAsTable("cacheTable")

hc.cacheTable("cacheTable")

hc.sql("select col2, max(col3) from cacheTable group by col2").collect()

hc.sql("select col3, min(col2) from cacheTable group by col3").collect()

# hc.uncacheTable("cacheTable")

sc.stop()
