from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("spark_sql_hive")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

try:
    rows = hc.sql("select b from yurun.tablep").collect()

    for row in rows:
        print row
except Exception, e:
    pass
finally:
    pass


sc.stop()
