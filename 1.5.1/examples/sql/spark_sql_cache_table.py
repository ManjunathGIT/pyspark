# coding=utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import Row
import random

conf = SparkConf().setAppName("spark_sql_cache_table")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

datas = [("col1_" + str(random.randint(1, 100)), "col2_" + str(random.randint(1, 100)),
          "col3_" + str(random.randint(1, 100))) for index in xrange(0, 10000)]

dataRDD = sc.parallelize(datas).map(lambda columns: Row(
    col1=columns[0], col2=columns[1], col3_=columns[2]))

sourceRDD = hc.inferSchema(dataRDD)

sourceRDD.registerAsTable("source")

cacheTableRDD = hc.sql(
    "select * from source where col1 = 'col1_50'")

cacheTableRDD.registerAsTable("cacheTable")

hc.cacheTable("cacheTable")

rows = hc.sql("select count(*) from cacheTable").collect()

rows = hc.sql("select count(*) from cacheTable").collect()

# hc.uncacheTable("cacheTable")

sc.stop()

for row in rows:
    print row
