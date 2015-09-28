from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_variance")

sc = SparkContext(conf=conf)

data = sc.parallelize([1, 2, 3]).variance()

sc.stop()

# 1.0
print data
