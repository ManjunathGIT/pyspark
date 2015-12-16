from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("spark_sql_hive")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

sc.stop()
