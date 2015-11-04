# coding=utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import Row

conf = SparkConf().setAppName("spark_sql_cache_table")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.parallelize(
    ['{"col1": "row1_col1","col2":"row1_col2","col3":"row1_col3"}', '{"col1": "row2_col1","col2":"row2_col2","col3":"row2_col3"}', '{"col1": "row3_col1","col2":"row3_col2","col3":"row3_col3"}'])


sourceRDD = hc.jsonRDD(source)

sourceRDD.registerAsTable("source")

cacheTableRDD = hc.sql("select upper(col1) as col1, col2, col3 from source")

cacheTableRDD.registerAsTable("cacheTable")

hc.cacheTable("cacheTable")

rows = hc.sql("select col1 from cacheTable").collect()
push
sc.stop()

for row in rows:
    print row
