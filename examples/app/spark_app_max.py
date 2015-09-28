from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_max")

sc = SparkContext(conf=conf)

data = sc.parallelize([1, 2, 3]).max()

data2 = sc.parallelize([("a", 1), ("b", 2), ("c", 3)]).max(lambda val: val[1])

sc.stop()

print data, data2
