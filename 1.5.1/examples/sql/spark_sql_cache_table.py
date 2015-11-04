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

sourceRDD.registerTempTable("source")

hc.cacheTable("source")

datas = hc.sql("select * from source").collect()

printRows(datas)

hc.uncacheTable("source")

sc.stop()
