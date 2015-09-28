from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_stats")

sc = SparkContext(conf=conf)

data = sc.parallelize([1, 2, 3]).stats()

sc.stop()

# 6
print data.count()
