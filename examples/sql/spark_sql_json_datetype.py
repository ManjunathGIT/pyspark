# coding: utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("spark_sql_json_datetime")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.parallelize(['{"key1" : 1, "key2" : "2"}'])

jsonRDD = hc.jsonRDD(source)

jsonRDD.registerTempTable("temp_table")

values = hc.sql("select key1, key2 from temp_table").collect()

sc.stop()

for value in values:
    print value
