from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("spark_sql_hive_udf")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

rows = hc.sql("show functions").collect()

sc.stop()

for row in rows:
    print row
