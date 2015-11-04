# coding=utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import Row

conf = SparkConf().setAppName("spark_sql_cache_table")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.parallelize(
    ['{"col1": "row1_col1","col2":"row1_col2","col3":"row1_col3"}', '{"col1": "row2_col1","col2":"row2_col2","col3":"row2_col3"}', '{"col1": "row3_col1","col2":"row3_col2","col3":"row3_col3"}'])


sourceRDD = hc.jsonRDD(source)

sourceRDD.registerAsTable("temp_source")


def upper_func(val):
    return val.upper()

hc.registerFunction("upper_func", upper_func)

cacheTableRDD = hc.sql(
    "select upper(col1) as col1, col2, col3 from temp_source")

cacheTableRDD.registerAsTable("temp_cacheTable")

# hc.cacheTable("temp_cacheTable")

rows = hc.sql("select col1 from temp_cacheTable").collect()

# hc.uncacheTable("cacheTable")

sc.stop()

for row in rows:
    print row
