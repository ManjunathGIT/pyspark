from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("spark_sql_hive")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

rows = hc.sql("select b from tablep").collect()

for row in rows:
    print row

sc.stop()
